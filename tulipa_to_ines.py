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
        resolutions = set()
        all_stamps = []
        for y in years:
            t0 = pd.Timestamp(year=y, month=1, day=1)
            offset = 0.0
            for k in sorted(reps.get(y, [1])):
                n = int(d.get("rep_periods_data", (y, k), "num_timesteps"))
                res = float(d.get("rep_periods_data", (y, k), "resolution"))
                resolutions.add(res)
                for t in range(1, n + 1):
                    ts = t0 + pd.Timedelta(hours=offset + (t - 1) * res)
                    self.stamp[(y, k, t)] = ts
                    all_stamps.append(ts)
                offset += n * res
            p = self.ent("period", self.period_name[y])
            self.put(p, "start_time", DateTime(t0.to_pydatetime()))
            self.put(p, "duration", Duration(f"{int(round(offset))}h"))
            i = years.index(y)
            self.put(p, "years_represented", float(years[i + 1] - y) if i + 1 < len(years) else 1.0)
            if len(reps.get(y, [])) > 1:
                self.report.note(f"Milestone year {y}: {len(reps[y])} representative periods are written one after another in the INES timeline (their weights are not represented).")
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

        simple = True
        for key in d.keys("rep_periods_mapping"):
            y, period, k, scen = key
            w = d.get("rep_periods_mapping", key, "weight")
            n_period = d.get("timeframe_data", (y, period), "num_timesteps")
            n_rep = d.get("rep_periods_data", (y, k), "num_timesteps")
            if len(reps.get(y, [])) == 1 and w == 1 and n_period == n_rep and scen == 1:
                continue
            simple = False
        if simple:
            for key in d.keys("rep_periods_mapping"):
                d.mark("rep_periods_mapping", key, "weight")
        for key in d.keys("timeframe_data"):
            d.mark("timeframe_data", key, "num_timesteps")
        for key in d.keys("rep_periods_data"):
            d.mark("rep_periods_data", key, "num_timesteps")
            d.mark("rep_periods_data", key, "resolution")

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
            if self.unit_like(f) and self.node_like(t):
                self.ines_flow[key] = [self.ent("unit__to_node", f, t)]
                if self.d.get("flow", key, "is_transport"):
                    self._skip_transport_attrs(key)
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

    def _skip_transport_attrs(self, key):
        reason = "Transport flow from a unit: INES unit flows are not transport links."
        tables = {"flow": [key], "flow_milestone": [k for k in self.d.keys("flow_milestone") if k[:2] == key],
                  "flow_commission": [k for k in self.d.keys("flow_commission") if k[:2] == key],
                  "flow_both": [k for k in self.d.keys("flow_both") if k[:2] == key]}
        cols = {"flow": ["capacity", "technical_lifetime", "economic_lifetime", "discount_rate", "investment_integer"],
                "flow_milestone": ["investable"], "flow_commission": ["fixed_cost", "investment_cost", "investment_max_limit", "investment_min_limit"],
                "flow_both": ["decommissionable", "initial_export_units", "initial_import_units"]}
        for table, keys in tables.items():
            for k in keys:
                for col in cols[table]:
                    if self.d.given(table, k, col) is not None:
                        self.report.skip(f"{table}.{col}", [k], reason)

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
        if len([k for k in outs if coefs[k]]) > 1:
            self.report.note(f"Unit {a}: Tulipa limits the weighted sum of outputs; INES gets one capacity per output flow.")
        self.put(u, "units_existing", self.year_map(self._existing(a, "initial_units")))
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
        if names:
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
        cap_e = d.get("asset", (a,), "capacity_storage_energy")
        method = d.get("asset", (a,), "storage_method_energy")
        if method == "optimize_storage_capacity":
            self._invest_retire(a, n, cap_e, "storages", "storage_investment_method", "storage_retirement_method", storage=True)
        else:
            fixed_ratio = method == "use_fixed_energy_to_power_ratio" and self._investable(a)
            self.put(n, "storage_investment_method", "no_limits" if fixed_ratio else "not_allowed")
            self.put(n, "storage_retirement_method", "economic_retirement" if self._decommissionable(a) else "not_retired")
        d.mark("asset", (a,), "storage_method_energy")
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
            if cap is not None and cap < self.big:
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
                if entry["role"] == "unit_flow" and entry["ines"] == "other_operational_cost" and ent[0] == "node__to_unit" and len(self.ines_flow[key]) > 1:
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
        if dy is None or dy == min(self.years):
            self.d.mark("model_parameters", (), "discount_year")

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
        self.classify()
        self.build_flows()
        self.build_assets()
        self.build_links()
        self.apply_direct()
        self.apply_min_operating_point()
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
