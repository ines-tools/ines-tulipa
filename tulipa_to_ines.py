"""Transform Tulipa input data (DuckDB file or CSV folder) into an INES model (Spine DB sqlite or JSON).

Usage:
    python tulipa_to_ines.py INPUT OUTPUT [--report REPORT.md]

INPUT  is a Tulipa DuckDB file or a folder with Tulipa CSV files.
OUTPUT is an INES sqlite file, a sqlite url (sqlite:///path) or a .json file.
"""

import argparse
import os
import sys
from collections import defaultdict

import numpy as np
import pandas as pd
from spinedb_api import Array, DateTime, Duration, Map, TimeSeriesVariableResolution

from ines_tulipa_common import (
    TULIPA_KEYS,
    Report,
    is_number,
    load_mapping,
    load_settings,
    load_tulipa_schema,
    partition_blocks,
    per_year_value,
    read_tulipa,
    write_ines,
)

UNIT_TYPES = ("producer", "conversion")


class TulipaData:
    """Indexed Tulipa tables; records which non-default values were used."""

    def __init__(self, frames, schema):
        self.schema = schema
        self.rows = {}
        for table, df in frames.items():
            keys = TULIPA_KEYS[table]
            index = {}
            for rec in df.to_dict("records"):
                key = tuple(_norm(rec[k]) for k in keys)
                index[key] = rec
            self.rows[table] = index
        self.used = set()

    def get(self, table, key, col, default=None, use_schema_default=True):
        row = self.rows[table].get(tuple(key))
        self.used.add((table, tuple(key), col))
        val = None if row is None else row.get(col)
        if val is None or (isinstance(val, float) and np.isnan(val)):
            if use_schema_default and default is None:
                return self.schema[table][col].get("default")
            return default
        return _norm(val)

    def given(self, table, key, col):
        """Value only if it differs from the Tulipa default (marks it used)."""
        val = self.get(table, key, col, use_schema_default=False)
        default = self.schema[table][col].get("default")
        if val is None or _same(val, default):
            return None
        return val

    def mark(self, table, key, col):
        self.used.add((table, tuple(key), col))

    def keys(self, table):
        return list(self.rows[table].keys())

    def unused(self):
        out = defaultdict(list)
        for table, index in self.rows.items():
            keycols = TULIPA_KEYS[table]
            for key, rec in index.items():
                for col, val in rec.items():
                    if col in keycols or col not in self.schema[table]:
                        continue
                    if val is None or (isinstance(val, float) and np.isnan(val)):
                        continue
                    if _same(_norm(val), self.schema[table][col].get("default")):
                        continue
                    if (table, key, col) not in self.used:
                        out[f"{table}.{col}"].append(key if len(key) != 1 else key[0])
        return out


def _norm(v):
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    return v


def _same(a, b):
    if a is None or b is None:
        return a is b
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b) and str(a).lower() == str(b).lower()
    if is_number(a) and is_number(b):
        return abs(float(a) - float(b)) < 1e-12
    return str(a) == str(b)


class TulipaToInes:
    def __init__(self, data: TulipaData, schema, mapping, settings, report: Report):
        self.d = data
        self.schema = schema
        self.mapping = mapping
        self.settings = settings
        self.report = report
        self.big = float(settings.get("unlimited_capacity", 1e6))
        self.entities = {}
        self.values = {}
        self.main_entity = {}
        self.flow_k = {}
        self.side_units = set()

    # ------------------------------------------------------------ output helpers
    def ent(self, cls, *byname):
        self.entities[(cls, tuple(byname))] = None
        return (cls, tuple(byname))

    def put(self, entity, param, value):
        if value is None:
            return
        self.values[(entity[0], entity[1], param)] = value

    def year_map(self, year_values):
        return per_year_value(year_values, self.period_name)

    # ------------------------------------------------------------ time
    def build_time(self):
        d = self.d
        reps = defaultdict(list)
        for (y, k) in d.keys("rep_periods_data"):
            reps[y].append(k)
        years = sorted(reps) or sorted({k[1] for k in d.keys("asset_milestone")})
        if not years:
            raise ValueError("No milestone years found in the Tulipa data.")
        self.years = years
        self.period_name = {y: str(y) for y in years}
        self.stamp = {}
        self.rep_steps = {}
        self.rep_name = {}
        resolutions = set()
        all_stamps = []
        for y in years:
            t0 = pd.Timestamp(year=y, month=1, day=1)
            offset = 0.0
            rep_hours = []
            for k in sorted(reps.get(y, [1])):
                n = int(d.get("rep_periods_data", (y, k), "num_timesteps"))
                res = float(d.get("rep_periods_data", (y, k), "resolution"))
                resolutions.add(res)
                self.rep_steps[(y, k)] = n
                for t in range(1, n + 1):
                    ts = t0 + pd.Timedelta(hours=offset + (t - 1) * res)
                    self.stamp[(y, k, t)] = ts
                    all_stamps.append(ts)
                offset += n * res
                rep_hours.append(n * res)
            p = self.ent("period", self.period_name[y])
            self.put(p, "start_time", DateTime(t0.to_pydatetime()))
            if len(rep_hours) > 1:
                self.put(p, "duration", Array([Duration(_hours_str(h)) for h in rep_hours]))
            else:
                self.put(p, "duration", Duration(f"{int(round(offset))}h"))
            i = years.index(y)
            self.put(p, "years_represented", float(years[i + 1] - y) if i + 1 < len(years) else 1.0)
            if len(reps.get(y, [])) > 1:
                self.report.note(f"Milestone year {y}: {len(reps[y])} representative periods are written one after another in the INES timeline (period.duration as an array).")
        self.res = sorted(resolutions)[0] if resolutions else 1.0
        if len(resolutions) > 1:
            self.report.note(f"Representative periods use several resolutions {sorted(resolutions)}; INES time_resolution set to {self.res} h.")
        sp = self.ent("solve_pattern", self.settings.get("ines_solve_pattern", "solve"))
        self.put(sp, "period", Array([self.period_name[y] for y in years]))
        self.put(sp, "time_resolution", Duration(_hours_str(self.res)))
        self.system = self.ent("system", self.settings.get("ines_system", "system"))
        self.put(self.system, "timeline", TimeSeriesVariableResolution(
            [s.to_pydatetime() for s in all_stamps], [1.0] * len(all_stamps), False, False))
        self.all_stamps = all_stamps

        totals = defaultdict(float)
        for key in d.keys("rep_periods_mapping"):
            y, period, k, scen = key
            totals[(y, k, scen)] += float(d.get("rep_periods_mapping", key, "weight") or 0.0)
        simple = all(abs(w - 1) < 1e-12 and s == 1 and len(reps.get(y, [])) == 1 for (y, k, s), w in totals.items())
        if simple:
            for key in d.keys("rep_periods_mapping"):
                d.mark("rep_periods_mapping", key, "weight")
        else:
            self._rep_period_mapping(sp, reps)
        for key in d.keys("timeframe_data"):
            d.mark("timeframe_data", key, "num_timesteps")
        for key in d.keys("rep_periods_data"):
            d.mark("rep_periods_data", key, "num_timesteps")
            d.mark("rep_periods_data", key, "resolution")

    def _rep_period_mapping(self, sp, reps):
        """Total weight of each representative period per period (and stochastic scenario) as an INES mapping."""
        d = self.d
        weights = defaultdict(float)
        for key in d.keys("rep_periods_mapping"):
            y, _, k, scen = key
            weights[(y, scen, k)] += float(d.get("rep_periods_mapping", key, "weight") or 0.0)
        for y in self.years:
            for k in sorted(reps.get(y, [])):
                self.rep_name[(y, k)] = f"{self.period_name[y]}_rp{k}"
        scens = sorted({s for _, s, _ in weights})

        def rep_map(y, s):
            ks = [k for k in sorted(reps.get(y, [])) if (y, s, k) in weights]
            return Map([self.rep_name[(y, k)] for k in ks], [weights[(y, s, k)] for k in ks], index_name="representative_period")

        periods = [self.period_name[y] for y in self.years]
        if len(scens) == 1:
            self.put(sp, "representative_period_mapping", Map(periods, [rep_map(y, scens[0]) for y in self.years], index_name="period"))
        else:
            self.put(sp, "stochastic_representative_period_mapping", Map(periods, [
                Map([str(s) for s in scens], [rep_map(y, s) for s in scens], index_name="stochastic_branch") for y in self.years
            ], index_name="period"))
        self.report.note("Representative period weights summed over the Tulipa timeframe periods (the order of the periods, "
                         "used by inter-period constraints, is not represented).")

    def rep_profile_map(self, names_by_year):
        """INES 2D map representative period -> timestep -> value from Tulipa profiles of each milestone year."""
        reps, values = [], []
        for (y, k), rep in sorted(self.rep_name.items()):
            name = names_by_year.get(y)
            n = self.rep_steps[(y, k)]
            vals = [self.d.get("profiles_rep_periods", (name, y, k, t), "value", default=1.0) for t in range(1, n + 1)] if name else [1.0] * n
            reps.append(rep)
            values.append(Map([str(t) for t in range(1, n + 1)], [float(v) for v in vals], index_name="timestep"))
        return Map(reps, values, index_name="representative_period")

    def _profile_values(self, name, y):
        """Values of a profile for a milestone year in timeline order (None if missing)."""
        vals = []
        for (pname, yy, k, t) in self._profile_index.get((name, y), []):
            vals.append((self.stamp.get((yy, k, t)), self.d.get("profiles_rep_periods", (pname, yy, k, t), "value")))
        if not vals:
            return None
        return sorted([v for v in vals if v[0] is not None], key=lambda x: x[0])

    def profile_series(self, names_by_year, scale_by_year=None, fill=1.0):
        """INES time series from Tulipa profiles of each milestone year (times a scale)."""
        stamps, values = [], []
        for y in self.years:
            scale = 1.0 if scale_by_year is None else (scale_by_year.get(y) or 0.0)
            name = names_by_year.get(y)
            data = self._profile_values(name, y) if name else None
            year_stamps = [s for s in self.all_stamps if s.year == y] if not data else [s for s, _ in data]
            year_vals = [fill] * len(year_stamps) if not data else [float(v) for _, v in data]
            if not data and name:
                self.report.note(f"Profile {name} has no data for {y}; {fill} used.")
            stamps.extend(year_stamps)
            values.extend(v * scale for v in year_vals)
        return TimeSeriesVariableResolution([s.to_pydatetime() for s in stamps], values, False, False)

    def asset_profile_names(self, a, ptype):
        rows = [k for k in self.d.keys("assets_profiles") if k[0] == a and k[2] == ptype]
        if not rows:
            return {}
        out = {}
        for y in self.years:
            cands = [r for r in rows if r[1] == y] or [r for r in rows if r[1] <= y] or rows
            r = max(cands, key=lambda r: r[1])
            out[y] = self.d.get("assets_profiles", r, "profile_name")
        if len({r[1] for r in rows}) > 1 and len(set(out.values())) > 1 and any(r[1] not in self.years for r in rows):
            self.report.note(f"Asset {a}: vintage specific {ptype} profiles reduced to one profile per milestone year.")
        for r in rows:
            self.d.mark("assets_profiles", r, "profile_name")
        return out

    def flow_profile_names(self, key, ptype):
        out = {}
        for k in self.d.keys("flows_profiles"):
            if k[0] == key[0] and k[1] == key[1] and k[3] == ptype:
                out[k[2]] = self.d.get("flows_profiles", k, "profile_name")
        return out

    # ------------------------------------------------------------ vintages
    def split_vintages(self):
        """Compact vintage units become one aggregated asset per commission year, named <asset>_<year>."""
        R = self.d.rows
        asset_cols = ("asset", "from_asset", "to_asset", "flow_1_from_asset", "flow_1_to_asset", "flow_2_from_asset", "flow_2_to_asset")

        def pick(by_year, c):
            """Row of commission year c, else of the latest earlier year, else of the first year."""
            if not by_year:
                return None
            earlier = [y for y in by_year if y <= c]
            return by_year[c] if c in by_year else by_year[max(earlier) if earlier else min(by_year)]

        for (a,), rec in list(R["asset"].items()):
            if rec.get("vintage_method") in (None, "aggregated") or rec.get("type") not in UNIT_TYPES:
                continue
            both = {k: r for k, r in R["asset_both"].items() if k[0] == a}
            comm = {k[1]: r for k, r in R["asset_commission"].items() if k[0] == a}
            vintages = []
            for c in sorted({k[2] for k in both} | set(comm)):
                existing = any((r.get("initial_units") or 0) > 0 for k, r in both.items() if k[2] == c)
                investable = c in self.years and bool((R["asset_milestone"].get((a, c)) or {}).get("investable"))
                if existing or investable:
                    vintages.append(c)
            if not vintages:
                continue
            for f, t in [k for k in R["flow"] if a in k and R["flow"][k].get("is_transport")]:
                self._shared_connection(a, f, t)
            flows = [k for k in R["flow"] if a in k]
            profiles = defaultdict(dict)
            for (pa, py, pt), r in R["assets_profiles"].items():
                if pa == a:
                    profiles[pt][py] = r
            for c in vintages:
                n = f"{a}_{c}"

                def ren(x):
                    return n if x == a else x

                R["asset"][(n,)] = {**rec, "asset": n, "vintage_method": "aggregated"}
                for y in self.years:
                    m = R["asset_milestone"].get((a, y))
                    if m:
                        R["asset_milestone"][(n, y)] = {**m, "asset": n, "investable": bool(m.get("investable")) and y == c}
                    src = pick(comm, c)
                    if src:
                        R["asset_commission"][(n, y)] = {**src, "asset": n, "commission_year": y}
                    b = both.get((a, y, c)) or {}
                    R["asset_both"][(n, y, y)] = {**b, "asset": n, "milestone_year": y, "commission_year": y,
                                                 "initial_units": b.get("initial_units") or 0.0}
                    for pt, by_year in profiles.items():
                        R["assets_profiles"][(n, y, pt)] = {**pick(by_year, c), "asset": n, "commission_year": y}
                for table in ("assets_rep_periods_partitions", "assets_timeframe_partitions", "assets_timeframe_profiles"):
                    for k, r in list(R[table].items()):
                        if k[0] == a:
                            R[table][(n,) + k[1:]] = {**r, "asset": n}
                for f, t in flows:
                    nf, nt = ren(f), ren(t)
                    R["flow"][(nf, nt)] = {**R["flow"][(f, t)], "from_asset": nf, "to_asset": nt}
                    fc = {k[2]: r for k, r in R["flow_commission"].items() if k[:2] == (f, t)}
                    for y in self.years:
                        src = pick(fc, c)
                        if src:
                            R["flow_commission"][(nf, nt, y)] = {**src, "from_asset": nf, "to_asset": nt, "commission_year": y}
                        fb = R["flow_both"].get((f, t, y, c))
                        if fb:
                            R["flow_both"][(nf, nt, y, y)] = {**fb, "from_asset": nf, "to_asset": nt, "commission_year": y}
                    for table in ("flow_milestone", "flows_profiles", "flows_rep_periods_partitions"):
                        for k, r in list(R[table].items()):
                            if k[:2] == (f, t):
                                R[table][(nf, nt) + k[2:]] = {**r, "from_asset": nf, "to_asset": nt}
                for k, r in list(R["flows_relationships"].items()):
                    if a in k[:4]:
                        nk = tuple(ren(x) for x in k[:4]) + k[4:]
                        R["flows_relationships"][nk] = {**r, **dict(zip(TULIPA_KEYS["flows_relationships"][:4], nk[:4]))}
                for k, r in list(R["investment_group_asset_membership"].items()):
                    if k[2] == a:
                        R["investment_group_asset_membership"][k[:2] + (n,)] = {**r, "asset": n}
            for table, index in R.items():
                pos = [i for i, col in enumerate(TULIPA_KEYS[table]) if col in asset_cols]
                for k in [k for k in index if any(k[i] == a for i in pos)]:
                    del index[k]
            self.report.note(f"Unit {a} ({rec.get('vintage_method')}): vintages {vintages} became separate units "
                             + ", ".join(f"{a}_{c}" for c in vintages) + " with their own commission year parameters.")

    def _shared_connection(self, a, f, t):
        """Transport flow of a vintage asset: one shared connection consumer between all vintages and the other asset."""
        R = self.d.rows
        hub = f"{f}__{t}"
        R["asset"][(hub,)] = {"asset": hub, "type": "consumer", "consumer_balance_sense": "=="}
        tk, uk = ((hub, t), (a, hub)) if f == a else ((f, hub), (hub, a))
        names = dict(zip(("from_asset", "to_asset"), tk))
        unit_names = dict(zip(("from_asset", "to_asset"), uk))
        for table in ("flow", "flow_milestone", "flow_commission", "flow_both", "flows_profiles", "flows_rep_periods_partitions"):
            for k, r in list(R[table].items()):
                if k[:2] != (f, t):
                    continue
                del R[table][k]
                R[table][tk + k[2:]] = {**r, **names}
                if table == "flow":
                    R[table][uk] = {**r, **unit_names, "is_transport": False}
                elif table == "flow_milestone":
                    # operational cost stays on the connection, commodity price on the unit flow
                    R[table][tk + k[2:]]["commodity_price"] = None
                    R[table][uk + k[2:]] = {**r, **unit_names, "operational_cost": None, "investable": None}
                elif table == "flow_commission":
                    R[table][uk + k[2:]] = {**r, **unit_names}
                    R[table][tk + k[2:]].update(capacity_coefficient=None, conversion_coefficient=None, producer_efficiency=None)
                elif table == "flows_rep_periods_partitions":
                    R[table][uk + k[2:]] = {**r, **unit_names}
        self.report.note(f"Transport flow {f}->{t} of vintage asset {a}: shared connection {hub} with the transport flow, each vintage flows into it.")

    def binary_storage_bus(self):
        """Binary storage without a single two-way connection: its flows move to a bus asset joined to the storage
        by two flows, which become one two-way INES link with exact_integer."""
        R = self.d.rows
        flow_tables = ("flow", "flow_milestone", "flow_commission", "flow_both", "flows_profiles", "flows_rep_periods_partitions")
        for (s,), rec in list(R["asset"].items()):
            if rec.get("type") != "storage" or rec.get("use_binary_storage_method") not in ("binary", "relaxed_binary"):
                continue
            ins = [k for k in R["flow"] if k[1] == s]
            outs = [k for k in R["flow"] if k[0] == s]
            others = {k[0] for k in ins} | {k[1] for k in outs}
            if len(ins) == 1 and len(outs) == 1 and len(others) == 1 \
                    and (R["asset"].get((others.pop(),)) or {}).get("type") in ("consumer", "storage"):
                continue
            coefs = [r.get("storage_coefficient") for k, r in R["flow_commission"].items() if k[:2] in ins + outs]
            if any(R["flow"][k].get("is_transport") for k in ins + outs) or any(c is not None and c == c and c != 1 for c in coefs):
                continue
            bus = f"{s}__bus"
            R["asset"][(bus,)] = {"asset": bus, "type": "consumer", "consumer_balance_sense": "=="}
            carrier = next((R["flow"][k].get("carrier") for k in ins + outs if R["flow"][k].get("carrier")), None)
            moved = {}
            for k in ins + outs:
                nk = (k[0], bus) if k[1] == s else (bus, k[1])
                moved[k] = nk
                for table in flow_tables:
                    for kk, r in list(R[table].items()):
                        if kk[:2] == k:
                            del R[table][kk]
                            R[table][nk + kk[2:]] = {**r, "from_asset": nk[0], "to_asset": nk[1]}
            for k, r in list(R["flows_relationships"].items()):
                if any(k[i:i + 2] in moved for i in (0, 2)):
                    nk = tuple(x for i in (0, 2) for x in moved.get(k[i:i + 2], k[i:i + 2])) + k[4:]
                    del R["flows_relationships"][k]
                    R["flows_relationships"][nk] = {**r, **dict(zip(TULIPA_KEYS["flows_relationships"][:4], nk[:4]))}
            for nk in ((bus, s), (s, bus)):
                R["flow"][nk] = {"from_asset": nk[0], "to_asset": nk[1], "carrier": carrier, "is_transport": False}
            self.report.note(f"Binary storage {s}: its flows go through a new balance node {bus}, joined to the storage by one two-way "
                             f"exact_integer link (power can also pass through {bus} without entering the storage).")

    # ------------------------------------------------------------ topology
    def classify(self):
        d = self.d
        self.atype = {a[0]: d.get("asset", a, "type") for a in d.keys("asset")}
        self.flows = d.keys("flow")
        up_sfx = self.settings.get("penalty_upward_suffix", "_penalty_upward")
        down_sfx = self.settings.get("penalty_downward_suffix", "_penalty_downward")
        self.penalty = {}
        for a, t in self.atype.items():
            outs = [f for f in self.flows if f[0] == a]
            ins = [f for f in self.flows if f[1] == a]
            if t == "producer" and a.endswith(up_sfx) and len(outs) == 1 and not ins and outs[0][1] == a[: -len(up_sfx)]:
                self.penalty[a] = ("penalty_upward", outs[0][1], outs[0])
            if t == "consumer" and a.endswith(down_sfx) and len(ins) == 1 and not outs and ins[0][0] == a[: -len(down_sfx)]:
                self.penalty[a] = ("penalty_downward", ins[0][0], ins[0])
        self.commodity_source = {
            a for a, t in self.atype.items()
            if t == "producer" and a not in self.penalty and (d.get("asset", (a,), "capacity") or 0) >= self.big
        }

    def node_like(self, a):
        return self.atype[a] in ("consumer", "storage") or a in self.commodity_source

    def unit_like(self, a):
        return self.atype[a] in UNIT_TYPES and a not in self.commodity_source and a not in self.penalty

    # ------------------------------------------------------------ flows
    def build_flows(self):
        self.ines_flow = {}
        self.links = {}
        self.storage_links = defaultdict(list)
        handled = set()
        penalty_flows = {p[2] for p in self.penalty.values()}
        for key in self.flows:
            f, t = key
            if key in penalty_flows or key in handled:
                continue
            if self.atype[t] == "producer" and t not in self.commodity_source:
                self.report.skip("flow (into a producer)", [key], "Tulipa producers cannot have inflows.")
                continue
            if self.d.get("flow", key, "is_transport") and (self.unit_like(f) or self.unit_like(t)):
                self._unit_transport_flow(key)
            elif self.atype[f] == "storage" and self.node_like(t) and self._storage_side_flow(key):
                pass
            elif self.unit_like(f) and self.node_like(t):
                self.ines_flow[key] = [self.ent("unit__to_node", f, t)]
            elif self.node_like(f) and self.unit_like(t):
                self.ines_flow[key] = [self.ent("node__to_unit", f, t)]
            elif self.unit_like(f) and self.unit_like(t):
                n = f"{f}__{t}"
                self.ent("node", n)
                self.put(("node", (n,)), "node_type", "balance")
                self.ines_flow[key] = [self.ent("unit__to_node", f, n), self.ent("node__to_unit", n, t)]
                self.report.note(f"Flow {f}->{t} between two units goes through a new INES balance node {n}.")
            else:
                transport = bool(self.d.get("flow", key, "is_transport"))
                rev = (t, f)
                two_way = not transport and rev in self.flows and not self.d.get("flow", rev, "is_transport") and rev not in penalty_flows
                l = f"{f}__{t}"
                self.ent("link", l)
                nln = self.ent("node__link__node", f, l, t)
                self.links[key] = {"link": ("link", (l,)), "nln": nln, "transport": transport, "reverse": rev if two_way else None}
                if two_way:
                    handled.add(rev)
                if transport:
                    method = "only_cost" if self._has_cost(key) else "no_losses_no_cost"
                elif two_way:
                    method = "regular_linear"
                else:
                    method = "one_way"
                self.put(("link", (l,)), "transfer_method", method)
                for s in (f, t):
                    if self.atype[s] == "storage" and not transport:
                        self.storage_links[s].append(key)
                if not transport and not any(self.atype[s] == "storage" for s in (f, t)):
                    self.report.note(f"Non-transport flow {f}->{t} between two nodes became a one_way INES link without capacity.")
            self.d.mark("flow", key, "is_transport")

    def _storage_side_flow(self, key):
        """Storage outflow with storage_coefficient 0 does not use stored energy: an INES unit without input sends it."""
        d = self.d
        coefs = [d.get("flow_commission", (*key, y), "storage_coefficient") for y in self.years]
        if any(c is None or c != 0 for c in coefs):
            return False
        f, t = key
        u = f"{f}__{t}"
        self.ent("unit", u)
        self.put(("unit", (u,)), "units_existing", 1.0)
        ent = self.ent("unit__to_node", u, t)
        self.ines_flow[key] = [ent]
        self.side_units.add(key)
        caps = {d.get("flow_commission", (*key, y), "capacity_coefficient") for y in self.years}
        if caps - {0, 0.0}:
            self.report.skip("flow_commission.capacity_coefficient", [key], "Storage flow with storage_coefficient 0 became a separate INES unit; its share of the storage power capacity is not represented.")
        self.put(ent, "capacity", self.big)
        for y in self.years:
            d.mark("flow_commission", (*key, y), "conversion_coefficient")
        self.report.note(f"Flow {f}->{t} with storage_coefficient 0 does not use the stored energy: INES unit {u} without input sends it to {t}.")
        return True

    def _unit_transport_flow(self, key):
        """Transport flow of a unit: unit flow to a connection balance node and a transport link with its own capacity."""
        f, t = key
        both = self.unit_like(f) and self.unit_like(t)
        nf = (f"{f}__{t}_from" if both else f"{f}__{t}") if self.unit_like(f) else f
        nt = (f"{f}__{t}_to" if both else f"{f}__{t}") if self.unit_like(t) else t
        ents = []
        for n in {nf, nt} - {f, t}:
            self.ent("node", n)
            self.put(("node", (n,)), "node_type", "balance")
        if self.unit_like(f):
            ents.append(self.ent("unit__to_node", f, nf))
        if self.unit_like(t):
            ents.append(self.ent("node__to_unit", nt, t))
        self.ines_flow[key] = ents
        l = f"{f}__{t}"
        self.ent("link", l)
        nln = self.ent("node__link__node", nf, l, nt)
        self.links[key] = {"link": ("link", (l,)), "nln": nln, "transport": True, "reverse": None}
        self.put(("link", (l,)), "transfer_method", "only_cost" if self._has_cost(key) else "no_losses_no_cost")
        self.report.note(f"Transport flow {f}->{t} of a unit: unit flow to the balance node {nf if self.unit_like(f) else nt} and a transport link {l} with the flow capacity.")

    def _has_cost(self, key):
        return any([(self.d.get("flow_milestone", (*key, y), "operational_cost") or 0) != 0 for y in self.years])

    # ------------------------------------------------------------ assets
    def build_assets(self):
        self._mark_not_applicable()
        for a, t in self.atype.items():
            self.d.mark("asset", (a,), "type")
            if a in self.penalty:
                continue
            if a in self.commodity_source:
                self._commodity_source(a)
            elif t in UNIT_TYPES:
                self._unit(a)
            elif t == "consumer":
                self._consumer(a)
            elif t == "storage":
                self._storage(a)
        for a, (param, n, key) in self.penalty.items():
            vals = {y: self.d.get("flow_milestone", (*key, y), "operational_cost") for y in self.years}
            self.put(("node", (n,)), param, self.year_map(vals))
            self.d.mark("asset", (a,), "capacity")
            for y in self.years:
                self.d.mark("asset_both", (a, y, y), "initial_units")
                self.d.mark("asset", (a,), "consumer_balance_sense")

    def _mark_not_applicable(self):
        """Mark columns that Tulipa ignores for the asset/flow type, so they are not reported."""
        d = self.d
        storage_cols = {
            "asset": ["capacity_storage_energy", "storage_method_energy", "investment_integer_storage_energy", "use_binary_storage_method", "energy_to_power_ratio", "inter_period_storage_level_bounds"],
            "asset_commission": ["fixed_cost_storage_energy", "investment_cost_storage_energy", "investment_max_limit_storage_energy", "investment_min_limit_storage_energy", "storage_charging_efficiency", "storage_discharging_efficiency", "storage_loss_from_stored_energy"],
            "asset_milestone": ["initial_storage_level", "storage_inflows"],
            "asset_both": ["initial_storage_units"],
        }
        consumer_cols = {
            "asset": ["capacity", "technical_lifetime", "economic_lifetime", "discount_rate", "investment_integer"],
            "asset_commission": ["fixed_cost", "investment_cost", "investment_max_limit", "investment_min_limit"],
            "asset_milestone": ["investable"],
            "asset_both": ["initial_units", "decommissionable"],
        }
        for table in ("asset", "asset_commission", "asset_milestone", "asset_both"):
            for key in d.keys(table):
                t = self.atype.get(key[0])
                cols = []
                if t != "storage":
                    cols += storage_cols[table]
                if t == "consumer":
                    cols += consumer_cols[table]
                if t != "conversion" and table == "asset_commission":
                    cols.append("conversion_efficiency")
                if table == "asset" and d.get("asset", key, "storage_method_energy") != "use_fixed_energy_to_power_ratio":
                    cols.append("energy_to_power_ratio")
                if t != "consumer" and table == "asset":
                    cols.append("consumer_balance_sense")
                for col in cols:
                    d.mark(table, key, col)
        transport_only = {"flow": ["capacity", "technical_lifetime", "economic_lifetime", "discount_rate", "investment_integer"],
                          "flow_commission": ["fixed_cost", "investment_cost", "investment_max_limit", "investment_min_limit"],
                          "flow_milestone": ["investable", "dc_opf", "reactance"]}
        for table, cols in transport_only.items():
            for key in d.keys(table):
                if not d.get("flow", key[:2], "is_transport"):
                    for col in cols:
                        d.mark(table, key, col)
        for key in d.keys("flow_commission"):
            if self.atype.get(key[0]) != "storage" and self.atype.get(key[1]) != "storage":
                d.mark("flow_commission", key, "storage_coefficient")

    def _existing(self, a, col):
        vals = defaultdict(float)
        vintages = defaultdict(int)
        for key in self.d.keys("asset_both"):
            if key[0] == a and key[1] in self.years:
                v = self.d.get("asset_both", key, col)
                vals[key[1]] += float(v or 0)
                vintages[key[1]] += 1
        if any(n > 1 for n in vintages.values()):
            self.report.note(f"Asset {a}: existing units of several commission years summed per milestone year.")
        return dict(vals)

    def _commission_date(self, table, prefix, col):
        """INES commission date when all existing units come from one commission year that differs from the milestone year."""
        n = len(prefix)
        rows = [k for k in self.d.keys(table) if k[:n] == tuple(prefix) and k[-2] in self.years]
        years = {k[-1] for k in rows if (self.d.get(table, k, col) or 0) > 0}
        if len(years) != 1:
            return None
        c = years.pop()
        if not any(k[-1] == c and k[-2] != c for k in rows):
            return None
        return DateTime(f"{c}-01-01T00:00:00")

    def apply_partitions(self):
        """Tulipa partitions become INES time_resolution (Duration, or Array of Durations if not constant) of the unit, node or link."""
        d = self.d
        reps = defaultdict(list)
        for (y, k) in d.keys("rep_periods_data"):
            reps[y].append(k)
        rep_info = {(y, k): (int(d.get("rep_periods_data", (y, k), "num_timesteps")), float(d.get("rep_periods_data", (y, k), "resolution")))
                    for y in reps for k in reps[y]}

        def blocks(table, key):
            """Block lengths in hours of one partition row (None if invalid)."""
            for col in ("specification", "partition"):
                d.mark(table, key, col)
            spec = d.get(table, key, "specification")
            b = partition_blocks(spec, d.get(table, key, "partition"))
            if not b:
                return None
            n, res = rep_info[key[-2:]]
            if spec == "uniform":
                b = [b[0]] * (n // b[0]) + ([n % b[0]] if n % b[0] else [])
            return [round(x * res, 9) for x in b]

        def resolution(rows):
            """INES time_resolution (hours, or tuple of hours) from {(y, k): blocks}; None if it differs between years."""
            seqs = set()
            for y in self.years:
                seq = []
                for k in sorted(reps.get(y, [])):
                    b = rows.get((y, k))
                    if b is None:
                        if (y, k) in rows:
                            return None
                        n, res = rep_info[(y, k)]
                        b = [round(res, 9)] * n
                    seq += b
                seqs.add(tuple(seq))
            if len(seqs) != 1:
                return None
            seq = next(iter(seqs))
            return seq[0] if len(set(seq)) == 1 else seq

        def to_ines(h):
            if isinstance(h, tuple):
                return Array([Duration(_hours_str(x)) for x in h])
            return Duration(_hours_str(h))

        def write(groups, what):
            for ent, rows in groups.items():
                h = resolution(rows)
                if h is None:
                    self.report.skip(what, [ent[1][0]], "INES time_resolution is the same for all periods; this partition is not.")
                elif h != round(self.res, 9):
                    self.put(ent, "time_resolution", to_ines(h))

        assets = defaultdict(dict)
        for key in d.keys("assets_rep_periods_partitions"):
            a, y, k = key
            if a in self.atype and (self.unit_like(a) or self.node_like(a)):
                assets[("unit" if self.unit_like(a) else "node", (a,))][(y, k)] = blocks("assets_rep_periods_partitions", key)
        unit_res = {ent[1][0]: resolution(rows) for ent, rows in assets.items() if ent[0] == "unit"}
        links, flow_rows = defaultdict(dict), defaultdict(dict)
        for key in d.keys("flows_rep_periods_partitions"):
            f, t, y, k = key
            b = blocks("flows_rep_periods_partitions", key)
            if (f, t) in self.ines_flow:
                flow_rows[(f, t)][(y, k)] = b
            if (f, t) in self.links:
                links[self.links[(f, t)]["link"]][(y, k)] = b
            elif self.links.get((t, f), {}).get("reverse") == (f, t):
                link = self.links[(t, f)]["link"]
                if links[link].get((y, k), b) != b:
                    self.report.skip("flows_rep_periods_partitions.partition", [key], "INES link has one time_resolution; the partition of the other direction is used.")
                    continue
                links[link][(y, k)] = b
            elif (f, t) not in self.ines_flow:
                self.report.skip("flows_rep_periods_partitions.partition", [key], "Flow has no INES counterpart.")
        # unit flows: INES flow time_resolution only where it differs from the unit's
        for key, ents in self.ines_flow.items():
            h = resolution(flow_rows.get(key, {}))
            if h is None:
                self.report.skip("flows_rep_periods_partitions.partition", [key], "INES time_resolution is the same for all periods; this partition is not.")
                continue
            for ent in ents:
                u = ent[1][0] if ent[0] == "unit__to_node" else ent[1][1]
                if h != unit_res.get(u, round(self.res, 9)):
                    self.put(ent, "time_resolution", to_ines(h))
        write(assets, "assets_rep_periods_partitions.partition")
        write(links, "flows_rep_periods_partitions.partition")

    def _decommissionable(self, a):
        return any([self.d.get("asset_both", k, "decommissionable") for k in self.d.keys("asset_both") if k[0] == a])

    def _investable(self, a):
        return any([self.d.get("asset_milestone", (a, y), "investable") for y in self.years])

    def _unit(self, a):
        d = self.d
        u = self.ent("unit", a)
        outs = [k for k in self.flows if k[0] == a and k in self.ines_flow]
        y0 = self.years[0]
        cap = d.get("asset", (a,), "capacity")
        coefs = {k: d.get("flow_commission", (*k, y0), "capacity_coefficient") for k in outs}
        main = next((k for k in outs if coefs[k] and coefs[k] > 0), outs[0] if outs else None)
        self.main_entity[a] = self.ines_flow[main][0] if main else None
        for k in outs:
            if coefs[k] and coefs[k] > 0 and cap:
                self.put(self.ines_flow[k][0], "capacity", float(cap) / coefs[k])
            for y in self.years:
                d.mark("flow_commission", (*k, y), "capacity_coefficient")
        self.put(u, "units_existing", self.year_map(self._existing(a, "initial_units")))
        self.put(u, "commission_date", self._commission_date("asset_both", (a,), "initial_units"))
        self._invest_retire(a, u, cap, "units", "investment_method", "retirement_method")
        if self.atype[a] == "conversion":
            self.put(u, "conversion_method", "constant_efficiency")
        else:
            self._commodity_for_producer(a, u, outs)
        uc = d.get("asset", (a,), "unit_commitment")
        if uc != "none":
            integer = d.get("asset", (a,), "unit_commitment_integer")
            self.put(u, "startup_method", "integer" if integer else "linear")
        else:
            d.mark("asset", (a,), "unit_commitment_integer")
            for col in ("minimum_up_time", "minimum_down_time"):
                d.mark("asset", (a,), col)
            for y in self.years:
                d.mark("asset_milestone", (a, y), "units_on_cost")
        if d.get("asset", (a,), "ramping"):
            if self.main_entity[a]:
                self.put(self.main_entity[a], "ramp_method", "ramp_limit")
        else:
            for col in ("max_ramp_up", "max_ramp_down"):
                d.mark("asset", (a,), col)
        names = self.asset_profile_names(a, "availability")
        if names and self.rep_name and len(outs) == 1:
            # with representative periods a single-output unit gets its availability in representative period format
            self.put(self.main_entity[a], "profile_limit_upper_representative_period", self.rep_profile_map(names))
            self.put(self.main_entity[a], "profile_method", "upper_limit")
        elif names:
            self.put(u, "availability", self.profile_series(names))

    def _commodity_for_producer(self, a, u, outs):
        d = self.d
        priced = [k for k in outs if any((d.get("flow_milestone", (*k, y), "commodity_price") or 0) != 0 for y in self.years)]
        if not priced:
            return False
        k = priced[0]
        if len(priced) > 1:
            self.report.note(f"Producer {a}: several output flows have a commodity price; the INES commodity node uses the price of {k}.")
        c = self.ent("node", a + self.settings.get("commodity_suffix", "_commodity"))
        self.put(c, "node_type", "commodity")
        prices = {y: d.get("flow_milestone", (*k, y), "commodity_price") for y in self.years}
        prof = self.flow_profile_names(k, "commodity_price")
        if prof:
            self.put(c, "commodity_price", self.profile_series(prof, prices))
            for y in prof:
                self.d.mark("flows_profiles", (*k, y, "commodity_price"), "profile_name")
        else:
            self.put(c, "commodity_price", self.year_map(prices))
        self.ent("node__to_unit", c[1][0], a)
        effs = {y: d.get("flow_commission", (*k, y), "producer_efficiency") for y in self.years}
        self.put(u, "efficiency", self.year_map(effs))
        self.put(u, "conversion_method", "constant_efficiency")
        for kk in priced:
            for y in self.years:
                d.mark("flow_milestone", (*kk, y), "commodity_price")
                d.mark("flow_commission", (*kk, y), "producer_efficiency")
        return True

    def _invest_retire(self, a, ent, cap, prefix, inv_param, ret_param, storage=False):
        d = self.d
        investable = self._investable(a)
        sfx = "_storage_energy" if storage else ""
        limits = {}
        for col, prm in (("investment_max_limit" + sfx, f"{prefix}_invest_max_period"), ("investment_min_limit" + sfx, f"{prefix}_invest_min_period")):
            vals = {}
            for y in self.years:
                v = d.given("asset_commission", (a, y), col) if investable else None
                if v is not None and cap:
                    vals[y] = float(v) / float(cap)
            if vals:
                limits[prm] = vals
        closed = [y for y in self.years if not d.get("asset_milestone", (a, y), "investable")]
        if investable and closed:
            # no investments in milestone years where the asset is not investable
            mx = limits.setdefault(f"{prefix}_invest_max_period", {})
            for y in self.years:
                mx[y] = 0.0 if y in closed else mx.get(y, self.big)
        cumulative = {}
        if not storage:
            for col, prm in (("max_available_units", f"{prefix}_max_cumulative"), ("min_available_units", f"{prefix}_min_cumulative")):
                vals = {y: d.get("asset_milestone", (a, y), col) for y in self.years}
                vals = {y: v for y, v in vals.items() if v is not None}
                if vals:
                    cumulative[prm] = vals
        if not investable:
            for y in self.years:
                for col in ("investment_max_limit" + sfx, "investment_min_limit" + sfx, "investment_cost" + sfx):
                    d.mark("asset_commission", (a, y), col)
        method = "not_allowed"
        if investable:
            method = {(False, False): "no_limits", (True, False): "period_limits", (False, True): "cumulative_limits", (True, True): "cumulative_and_period_limits"}[(bool(limits), bool(cumulative))]
        self.put(ent, inv_param, method)
        for prm, vals in {**limits, **cumulative}.items():
            self.put(ent, prm, self.year_map(vals))
        self.put(ent, ret_param, "economic_retirement" if self._decommissionable(a) else "not_retired")

    def _consumer(self, a):
        d = self.d
        n = self.ent("node", a)
        self.put(n, "node_type", "balance")
        peaks = {y: d.get("asset_milestone", (a, y), "peak_demand") or 0.0 for y in self.years}
        names = self.asset_profile_names(a, "demand")
        if any(peaks.values()):
            if names:
                self.put(n, "flow_profile", self.profile_series(names, {y: -p for y, p in peaks.items()}))
            else:
                self.put(n, "flow_profile", self.year_map({y: -p for y, p in peaks.items()}))
            self.put(n, "flow_scaling_method", "use_profile_directly")
        sense = d.get("asset", (a,), "consumer_balance_sense")
        if sense == ">=":
            self.put(n, "penalty_downward", 0.0)
        elif sense == "<=":
            self.put(n, "penalty_upward", 0.0)

    def _commodity_source(self, a):
        n = self.ent("node", a)
        self.put(n, "node_type", "commodity")
        outs = [k for k in self.flows if k[0] == a]
        prices = {}
        for k in outs:
            for y in self.years:
                p = self.d.get("flow_milestone", (*k, y), "commodity_price")
                if p:
                    prices.setdefault(y, p)
                self.d.mark("flow_milestone", (*k, y), "commodity_price")
                self.d.mark("flow_commission", (*k, y), "producer_efficiency")
        if prices:
            self.put(n, "commodity_price", self.year_map(prices))
        self.d.mark("asset", (a,), "capacity")
        for y in self.years:
            self.d.mark("asset_both", (a, y, y), "initial_units")

    def _storage(self, a):
        d = self.d
        n = self.ent("node", a)
        self.put(n, "node_type", "storage")
        self.put(n, "storages_existing", self.year_map(self._existing(a, "initial_storage_units")))
        self.put(n, "storage_commission_date", self._commission_date("asset_both", (a,), "initial_storage_units"))
        cap_e = d.get("asset", (a,), "capacity_storage_energy")
        method = d.get("asset", (a,), "storage_method_energy")
        if method == "optimize_storage_capacity":
            self._invest_retire(a, n, cap_e, "storages", "storage_investment_method", "storage_retirement_method", storage=True)
        else:
            fixed_ratio = method == "use_fixed_energy_to_power_ratio" and self._investable(a)
            self.put(n, "storage_investment_method", "no_limits" if fixed_ratio else "not_allowed")
            self.put(n, "storage_retirement_method", "economic_retirement" if self._decommissionable(a) else "not_retired")
        d.mark("asset", (a,), "storage_method_energy")
        if not d.get("asset", (a,), "use_inter_period_constraints") and self.rep_name:
            self.report.skip("asset.use_inter_period_constraints (false)", [a], "INES -> Tulipa links storages across representative periods by default.")
        levels = {y: d.get("asset_milestone", (a, y), "initial_storage_level") for y in self.years}
        if any(v is not None for v in levels.values()):
            self.put(n, "storage_state_fix", self.year_map(levels))
            self.put(n, "storage_state_fix_method", "fix_start")
        limits = []
        for ptype, prm, lim in (("max_storage_level", "storage_state_upper_limit", "upper"), ("min_storage_level", "storage_state_lower_limit", "lower")):
            names = self.asset_profile_names(a, ptype)
            if names:
                self.put(n, prm, self.profile_series(names, fill=1.0 if lim == "upper" else 0.0))
                limits.append(lim)
        if limits:
            self.put(n, "storage_limit_method", {("upper",): "upper_limit", ("lower",): "lower_limit"}.get(tuple(limits), "upper_and_lower_limit"))
        inflows = {y: d.get("asset_milestone", (a, y), "storage_inflows") or 0.0 for y in self.years}
        if any(inflows.values()):
            names = self.asset_profile_names(a, "inflows")
            value = self.profile_series(names, inflows) if names else self.year_map(inflows)
            self.put(n, "flow_profile", value)
            self.put(n, "flow_scaling_method", "use_profile_directly")
        cap = d.get("asset", (a,), "capacity")
        links = self.storage_links.get(a, [])
        direct = any(a in k and self.unit_like(k[0] if k[1] == a else k[1]) for k in self.ines_flow)
        binary = d.given("asset", (a,), "use_binary_storage_method")
        if binary:
            two_way = [k for k in links if self.links[k]["reverse"]]
            for k in two_way:
                self.put(self.links[k]["link"], "transfer_method", "exact_integer")
            if not two_way:
                self.report.skip("asset.use_binary_storage_method", [a], "INES uses transfer_method exact_integer on a two-way link between the storage and a node; this storage has none.")
            elif binary == "relaxed_binary":
                self.report.note(f"Storage {a}: relaxed_binary written as exact_integer (INES has no relaxed version).")
        if not links:
            if cap is not None and cap < self.big and not direct:
                self.report.skip("asset.capacity (storage power)", [a], "Storage has no link to a node: INES storage nodes have no power capacity (limit it in the connected units).")
            else:
                d.mark("asset", (a,), "capacity")
            for y in self.years:
                d.mark("asset_both", (a, y, y), "initial_units")
            if method == "use_fixed_energy_to_power_ratio":
                self.report.skip("asset.energy_to_power_ratio", [a], "INES expresses it as a constraint between the storage and its link; this storage has no link to a node.")
            return
        if len(links) > 1:
            self.report.note(f"Storage {a} has several links; each INES link gets the full storage power capacity.")
        existing = self._existing(a, "initial_units")
        for key in links:
            info = self.links[key]
            link = info["link"]
            self.put(link, "capacity", cap)
            self.put(link, "links_existing", self.year_map(existing))
            self.put(link, "commission_date", self._commission_date("asset_both", (a,), "initial_units"))
            self._invest_retire(a, link, cap, "links", "investment_method", "retirement_method")
            for prm in ("investment_cost", "fixed_cost"):
                vals = {y: d.given("asset_commission", (a, y), prm) for y in self.years}
                self.put(link, prm, self.year_map(vals))
            integer = d.given("asset", (a,), "investment_integer")
            if integer is not None:
                self.put(link, "investment_uses_integer", bool(integer))
            ch = {y: d.get("asset_commission", (a, y), "storage_charging_efficiency") for y in self.years}
            dis = {y: d.get("asset_commission", (a, y), "storage_discharging_efficiency") for y in self.years}
            f, t = key
            into_storage_first = t == a
            first, second = (ch, dis) if into_storage_first else (dis, ch)
            self.put(info["nln"], "efficiency", self.year_map(first))
            if info["reverse"]:
                self.put(link, "efficiency", self.year_map(second))
            elif any(v != 1 for v in second.values()):
                self.report.note(f"Storage {a}: one-directional link {f}->{t}, the efficiency of the other direction is not used.")
        if method == "use_fixed_energy_to_power_ratio":
            self._energy_to_power_constraint(a, links[0], cap, cap_e)

    def _energy_to_power_constraint(self, a, link_key, cap, cap_e):
        """INES: link_count_coefficient * links - storage_count = 0, i.e. storage units per link unit."""
        e2p = self.d.get("asset", (a,), "energy_to_power_ratio")
        if not e2p or not cap or not cap_e:
            self.report.skip("asset.energy_to_power_ratio", [a], "Needs energy_to_power_ratio, power capacity and capacity_storage_energy above zero.")
            return
        c = self.ent("constraint", f"{a}_energy_to_power")
        self.put(c, "sense", "equal")
        self.put(c, "constant", 0.0)
        self.put(("node", (a,)), "constraint_storage_count_coefficient", Map([c[1][0]], [-1.0], index_name="constraint"))
        self.put(self.links[link_key]["link"], "constraint_link_count_coefficient",
                 Map([c[1][0]], [float(e2p) * float(cap) / float(cap_e)], index_name="constraint"))
        if len(self.storage_links.get(a, [])) > 1:
            self.report.note(f"Storage {a}: energy-to-power constraint written for link {self.links[link_key]['link'][1][0]} only.")

    # ------------------------------------------------------------ flow and link parameters
    def build_links(self):
        d = self.d
        for key, info in self.links.items():
            link, nln = info["link"], info["nln"]
            carrier = d.given("flow", key, "carrier")
            self.put(nln, "carrier", carrier)
            if info["reverse"]:
                rc = d.given("flow", info["reverse"], "carrier")
                if rc is not None and rc != carrier:
                    self.report.skip("flow.carrier", [info["reverse"]], "INES link has one carrier; the carrier of the other direction is used.")
            costs = {y: d.given("flow_milestone", (*key, y), "operational_cost") for y in self.years}
            if info["reverse"]:
                rcosts = {y: d.given("flow_milestone", (*info["reverse"], y), "operational_cost") for y in self.years}
                self.put(nln, "operational_cost", self.year_map(costs))
                self.put(link, "operational_cost", self.year_map(rcosts))
            elif not info["transport"]:
                self.put(link, "operational_cost", self.year_map(costs))
            if not info["transport"]:
                continue
            exp = {}
            for k in d.keys("flow_both"):
                if (k[0], k[1]) == key and k[2] in self.years:
                    e = d.get("flow_both", k, "initial_export_units") or 0.0
                    i = d.get("flow_both", k, "initial_import_units") or 0.0
                    exp[k[2]] = exp.get(k[2], 0.0) + e
                    if e != i:
                        self.report.skip("flow_both.initial_import_units", [key], "INES links have the same number of units in both directions; export units used.")
            self.put(link, "links_existing", self.year_map(exp))
            self.put(link, "commission_date", self._commission_date("flow_both", key, "initial_export_units"))
            investable = any([d.get("flow_milestone", (*key, y), "investable") for y in self.years])
            cap = d.get("flow", key, "capacity")
            limits = {}
            for col, prm in (("investment_max_limit", "links_invest_max_period"), ("investment_min_limit", "links_invest_min_period")):
                vals = {y: d.given("flow_commission", (*key, y), col) for y in self.years}
                vals = {y: v / cap for y, v in vals.items() if v is not None and cap}
                if vals:
                    limits[prm] = vals
            self.put(link, "investment_method", ("period_limits" if limits else "no_limits") if investable else "not_allowed")
            for prm, vals in limits.items():
                self.put(link, prm, self.year_map(vals))
            decom = any([d.get("flow_both", k, "decommissionable") for k in d.keys("flow_both") if (k[0], k[1]) == key])
            self.put(link, "retirement_method", "economic_retirement" if decom else "not_retired")
            names = self.flow_profile_names(key, "availability")
            if names:
                self.put(link, "availability", self.profile_series(names))
                for y in names:
                    d.mark("flows_profiles", (*key, y, "availability"), "profile_name")

    # ------------------------------------------------------------ direct parameters
    def apply_direct(self):
        targets = defaultdict(list)
        for a in self.atype:
            if self.unit_like(a):
                targets["unit"].append(((a,), ("unit", (a,))))
                if self.main_entity.get(a):
                    targets["unit_main"].append(((a,), self.main_entity[a]))
            elif self.atype[a] == "storage":
                targets["storage"].append(((a,), ("node", (a,))))
        for key, ents in self.ines_flow.items():
            for e in ents:
                targets["unit_flow"].append((key, e))
        for key, info in self.links.items():
            if info["transport"]:
                targets["link"].append((key, info["link"]))
        targets["system"].append(((), self.system))
        uc_off = {a for a in self.atype if self.d.get("asset", (a,), "unit_commitment") == "none"}
        ramp_off = {a for a in self.atype if not self.d.get("asset", (a,), "ramping")}
        for entry in self.mapping["direct_parameters"]:
            factor = float(entry.get("factor", 1))
            table, col = entry["tulipa"][0]
            for key, ent in targets[entry["role"]]:
                if entry["ines"] in ("min_uptime", "min_downtime", "online_cost") and key[0] in uc_off:
                    continue
                if entry["ines"] in ("ramp_limit_up", "ramp_limit_down") and key[0] in ramp_off:
                    continue
                if entry["ines"] == "efficiency" and self.atype[key[0]] == "producer":
                    for y in self.years:
                        self.d.mark("asset_commission", (key[0], y), "conversion_efficiency")
                    continue
                if entry["role"] == "unit_flow" and entry["ines"] == "other_operational_cost" and (key in self.links or (ent[0] == "node__to_unit" and len(self.ines_flow[key]) > 1)):
                    continue
                if entry["ines"] == "conversion_coefficient" and key in self.side_units:
                    continue
                if table in ("asset", "flow", "model_parameters"):
                    v = self.d.given(table, key, col)
                    value = None if v is None else _scale(v, 1 / factor)
                else:
                    vals = {y: self.d.given(table, (*key, y), col) for y in self.years}
                    value = self.year_map({y: _scale(v, 1 / factor) for y, v in vals.items() if v is not None})
                self.put(ent, entry["ines"], value)
                for t2, c2 in entry["tulipa"][1:]:
                    if t2 not in ("asset", "flow") or table not in ("asset", "flow"):
                        continue
                    other, first = self.d.get(t2, key, c2), self.d.get(table, key, col)
                    if other is not None and first is not None and not _same(other, first):
                        self.report.skip(f"{t2}.{c2}", [key], f"INES has one {entry['ines']}; {table}.{col} used.")
        dy = self.d.get("model_parameters", (), "discount_year")
        if dy is not None and dy != min(self.years):
            self.put(self.system, "discount_year", float(dy))

    def _scale_years(self, value, factor):
        """Constant or period map times a factor per milestone year."""
        if is_number(value) and len(set(factor.values())) == 1:
            return float(value) * next(iter(factor.values()))
        if is_number(value):
            return self.year_map({y: float(value) * f for y, f in factor.items()})
        if isinstance(value, Map):
            year_of = {self.period_name[y]: y for y in self.years}
            return Map(list(value.indexes), [_scale(v, factor.get(year_of.get(i), 1.0)) for i, v in zip(value.indexes, value.values)],
                       index_name=value.index_name)
        return value

    def apply_storage_unit_flows(self):
        """Tulipa applies storage efficiency and power capacity also to flows between a storage and a unit.
        INES measures the flow at the storage: G = k * F, k = charging efficiency or 1 / discharging efficiency."""
        d = self.d
        y0 = self.years[0]

        def units(a):
            return sum(float(d.get("asset_both", kk, "initial_units") or 0) for kk in d.keys("asset_both") if kk[0] == a and kk[1] == y0)

        for key, ents in self.ines_flow.items():
            f, t = key
            charging = self.atype[t] == "storage"
            if len(ents) != 1 or key in self.links or key in self.side_units or not (charging or self.atype[f] == "storage"):
                continue
            s, u = (t, f) if charging else (f, t)
            ent = ents[0]
            col = "storage_charging_efficiency" if charging else "storage_discharging_efficiency"
            eff = {y: float(d.get("asset_commission", (s, y), col) or 1.0) for y in self.years}
            k = {y: e if charging else 1.0 / e for y, e in eff.items()}
            inv_k = {y: 1.0 / v for y, v in k.items()}
            self.flow_k[key] = k[y0]
            main = self.main_entity.get(u) == ent
            # stricter of the unit and the storage power capacity (per unit of the unit, existing units of the first year)
            u_cap = self.values.get((*ent, "capacity"))
            cap = float(u_cap) if is_number(u_cap) else None
            s_cap = d.get("asset", (s,), "capacity")
            if is_number(s_cap) and s_cap < self.big:
                n_u, n_s = units(u), units(s)
                s_per_unit = float(s_cap) * n_s / n_u if n_u else float(s_cap)
                if cap is None or s_per_unit < cap:
                    cap = s_per_unit
                    if self._investable(u) or self._investable(s):
                        self.report.note(f"Flow {f}->{t}: storage {s} power capacity used as the flow capacity based on existing units; investments change it in Tulipa.")
            if cap is not None:
                self.put(ent, "capacity", cap * k[y0])
            if any(v != 1 for v in eff.values()):
                if len(set(k.values())) > 1:
                    self.report.note(f"Flow {f}->{t}: storage efficiency varies by year; the flow capacity uses the first year.")
                eff_key = ("unit", (u,), "efficiency")
                if self.atype[u] == "conversion" or eff_key in self.values:
                    side = [kk for kk in self.ines_flow if (kk[0] == u if charging else kk[1] == u)]
                    if len(side) == 1:
                        self.values[eff_key] = self._scale_years(self.values.get(eff_key, 1.0), eff)
                    else:
                        cc = (*ent, "conversion_coefficient")
                        self.values[cc] = self._scale_years(self.values.get(cc, 1.0), inv_k)
                for prm in ["other_operational_cost"] + (["investment_cost", "fixed_cost"] if main else []):
                    if (*ent, prm) in self.values:
                        self.values[(*ent, prm)] = self._scale_years(self.values[(*ent, prm)], inv_k)
            self.report.note(f"Flow {f}->{t}: storage {s} {'charging' if charging else 'discharging'} efficiency and power capacity applied to the INES unit flow.")

    def apply_combined_capacity(self):
        """Tulipa limits sum(capacity_coefficient * output) by the asset capacity: INES combined_capacity and capacity_coefficient."""
        y0 = self.years[0]
        for a in self.atype:
            if not self.unit_like(a):
                continue
            outs = [k for k in self.flows if k[0] == a and k in self.ines_flow]
            cap = self.d.get("asset", (a,), "capacity")
            coefs = {k: self.d.get("flow_commission", (*k, y0), "capacity_coefficient") or 0.0 for k in outs}
            limited = [k for k in outs if coefs[k] > 0]
            main = next((k for k in outs if self.ines_flow[k][0] == self.main_entity.get(a)), None)
            # INES main flow capacity is cap / coefficient, so costs per MW scale by the coefficient
            if main and coefs[main] > 0 and abs(coefs[main] - 1) > 1e-12:
                for prm in ("investment_cost", "fixed_cost"):
                    key = (*self.main_entity[a], prm)
                    if key in self.values:
                        self.values[key] = self._scale_years(self.values[key], {y: coefs[main] for y in self.years})
            if len(limited) < 2 or not cap:
                continue
            # sum(c * output) <= combined_capacity; storage flows are measured at the storage (G = k * F)
            self.put(("unit", (a,)), "combined_capacity", float(cap))
            for k in outs:
                c = coefs[k] / self.flow_k.get(k, 1.0)
                if abs(c - 1) > 1e-12:
                    self.put(self.ines_flow[k][0], "capacity_coefficient", c)
                if len({self.d.get("flow_commission", (*k, y), "capacity_coefficient") for y in self.years}) > 1:
                    self.report.note(f"Flow {k[0]}->{k[1]}: capacity_coefficient varies by commission year; the first year is used.")

    def apply_min_operating_point(self):
        """INES has the minimum load as the lowest operating point of an efficiency map."""
        for a in self.atype:
            if not self.unit_like(a):
                continue
            mop = self.d.given("asset", (a,), "min_operating_point")
            if mop is None:
                continue
            key = ("unit", (a,), "efficiency")
            eff = self.values.get(key, 1.0)

            def curve(e):
                return Map([float(mop), 1.0], [float(e), float(e)], index_name="operating_point")

            if isinstance(eff, Map):
                eff = Map(list(eff.indexes), [curve(e) for e in eff.values], index_name=eff.index_name)
            else:
                eff = curve(eff)
            self.values[key] = eff
            self.put(self.ent("unit", a), "conversion_method", self.values.get(("unit", (a,), "conversion_method"), "constant_efficiency"))

    # ------------------------------------------------------------ relationships and groups
    def build_relationships(self):
        d = self.d
        sense_to = {"==": ("equality_ratio", "equality_constant"), ">=": ("greater_than_ratio", "greater_than_constant"), "<=": ("less_than_ratio", "less_than_constant")}
        rel = defaultdict(dict)
        for k in d.keys("flows_relationships"):
            rel[k[:4]][k[4]] = k
        for pair, by_year in rel.items():
            f1, f2 = (pair[0], pair[1]), (pair[2], pair[3])
            e1 = self.ines_flow.get(f1, [None])[0]
            e2 = self.ines_flow.get(f2, [None])[0]
            if e1 is None or e2 is None:
                for k in by_year.values():
                    self.report.skip("flows_relationships", [k], "INES flow relationships are only defined between unit flows.")
                    for col in ("ratio", "constant", "sense"):
                        d.mark("flows_relationships", k, col)
                continue
            ent = self.ent("unit_flow__unit_flow", *e1[1], *e2[1])
            senses = {d.get("flows_relationships", k, "sense") for k in by_year.values()}
            if len(senses) > 1:
                self.report.note(f"Flow relationship {pair}: different senses per year; the first is used.")
            rp, cp = sense_to[sorted(senses)[0]]
            self.put(ent, rp, self.year_map({y: d.get("flows_relationships", k, "ratio") for y, k in by_year.items()}))
            consts = {y: d.get("flows_relationships", k, "constant") for y, k in by_year.items()}
            if any(consts.values()):
                self.put(ent, cp, self.year_map(consts))

    def build_groups(self):
        d = self.d
        plan = {("use_only_investment_units", "<="): ["invest_max_period"], ("use_only_investment_units", ">="): ["invest_min_period"],
                ("use_only_investment_units", "=="): ["invest_max_period", "invest_min_period"],
                ("use_available_units", "<="): ["max_cumulative"], ("use_available_units", ">="): ["min_cumulative"],
                ("use_available_units", "=="): ["max_cumulative", "min_cumulative"]}
        groups = defaultdict(dict)
        for k in d.keys("investment_group_asset"):
            groups[k[0]][k[1]] = k
        members = defaultdict(list)
        for k in d.keys("investment_group_asset_membership"):
            members[k[0]].append(k)
        for name, by_year in groups.items():
            k0 = next(iter(by_year.values()))
            method = d.get("investment_group_asset", k0, "invest_method")
            sense = d.get("investment_group_asset", k0, "constraint_sense")
            ok = (method, sense) in plan
            mem_assets = sorted({m[2] for m in members[name]})
            for m in members[name]:
                coef = d.get("investment_group_asset_membership", m, "coefficient")
                if not self.unit_like(m[2]) or abs((d.get("asset", (m[2],), "capacity") or 0) - (coef or 0)) > 1e-9:
                    ok = False
            if not ok:
                for k in by_year.values():
                    self.report.skip("investment_group_asset", [k], "Only groups of units with coefficient = asset capacity (MW based) map to INES sets.")
                continue
            s = self.ent("set", name)
            for a in mem_assets:
                self.ent("set__unit", name, a)
            for m in members[name]:
                d.mark("investment_group_asset_membership", m, "coefficient")
            rhs = {y: d.get("investment_group_asset", k, "rhs") for y, k in by_year.items()}
            for prm in plan[(method, sense)]:
                self.put(s, prm, self.year_map(rhs))
            for k in by_year.values():
                for col in ("invest_method", "constraint_sense", "rhs"):
                    d.mark("investment_group_asset", k, col)

    # ------------------------------------------------------------ run
    def _skip_non_milestone_vintages(self):
        """Report rows of commission years that are not milestone years under one explanation."""
        d = self.d
        reason = "Commission year is not a milestone year: INES has no vintages before/between its periods."
        for table, pos in (("asset_commission", 1), ("flow_commission", 2), ("asset_both", 2), ("flow_both", 3)):
            for key in d.keys(table):
                if key[pos] in self.years:
                    continue
                for col in self.schema[table]:
                    if col in TULIPA_KEYS[table] or (table, key, col) in d.used:
                        continue
                    if d.given(table, key, col) is not None:
                        self.report.skip(f"{table}.{col}", [key], reason)

    def run(self):
        self._profile_index = defaultdict(list)
        for k in self.d.keys("profiles_rep_periods"):
            self._profile_index[(k[0], k[1])].append(k)
        self.build_time()
        self.split_vintages()
        self.binary_storage_bus()
        self.classify()
        self.build_flows()
        self.build_assets()
        self.build_links()
        self.apply_direct()
        self.apply_storage_unit_flows()
        self.apply_combined_capacity()
        self.apply_min_operating_point()
        self.apply_partitions()
        self.build_relationships()
        self.build_groups()
        self._skip_non_milestone_vintages()
        for name, keys in self.d.unused().items():
            self.report.skip(name, keys)
        entities = sorted(self.entities, key=lambda e: len(e[1]))
        values = [(c, b, p, v) for (c, b, p), v in self.values.items()]
        return entities, values


def _scale(v, f):
    if is_number(v):
        return float(v) * f
    return v


def _hours_str(h):
    if float(h).is_integer():
        return f"{int(h)}h"
    return f"{int(round(h * 60))}m"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Transform Tulipa input data into INES.")
    parser.add_argument("input", help="Tulipa DuckDB file or folder with Tulipa CSV files")
    parser.add_argument("output", help="INES sqlite file/url or .json file")
    parser.add_argument("--report", help="Conversion report (markdown). Default: next to the output")
    args = parser.parse_args(argv)

    mapping = load_mapping()
    settings = load_settings()
    schema = load_tulipa_schema()
    report = Report(f"Tulipa -> INES conversion report for {os.path.basename(os.path.normpath(args.input))}", mapping["not_transformed_reasons"]["tulipa"])
    frames, extra = read_tulipa(args.input, schema)
    for table, df in extra.items():
        if len(df):
            report.skip(f"{table} (table)", [table], "Not a Tulipa input table of this version (e.g. profiles-wide needs TulipaClustering first).")
    data = TulipaData(frames, schema)
    entities, values = TulipaToInes(data, schema, mapping, settings, report).run()
    write_ines(args.output, entities, values, settings.get("ines_alternative", "Base"), settings.get("ines_scenario", "base"))
    report_path = args.report or os.path.splitext(args.output.split("///")[-1])[0] + "_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report.to_markdown())
    report.print_summary()
    print(f"INES data written to {args.output}; report: {report_path}")


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    main()
