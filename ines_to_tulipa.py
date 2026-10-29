"""Transform an INES model (Spine DB sqlite or JSON) into Tulipa input data (DuckDB and/or CSV).

Usage:
    python ines_to_tulipa.py INPUT [--duckdb OUT.duckdb] [--csv OUT_DIR]
                             [--scenario NAME | --alternative ALT ...] [--report REPORT.md]

INPUT is an INES sqlite file, a sqlite url (sqlite:///path) or an INES JSON export.
"""

import argparse
import os
import re
import sys
from collections import defaultdict

import numpy as np
import pandas as pd
from spinedb_api import Array, Map, TimeSeries

from ines_tulipa_common import (
    InesData,
    Report,
    TulipaBuilder,
    blocks_to_math,
    ceil_int,
    duration_hours,
    is_number,
    load_mapping,
    load_settings,
    load_tulipa_schema,
    map_to_dict,
    open_ines_source,
    partition_blocks,
    to_timestamp,
    ts_to_series,
    value_list,
    write_tulipa,
)

STORAGE = "storage"
COMMODITY = "commodity"


class InesToTulipa:
    def __init__(self, ines: InesData, schema, mapping, settings, report: Report):
        self.ines = ines
        self.schema = schema
        self.mapping = mapping
        self.settings = settings
        self.report = report
        self.b = TulipaBuilder(schema)
        self.big = float(settings.get("unlimited_capacity", 1e6))
        self.flow_info = {}
        self.ines_flow_to_tulipa = {}

    # ------------------------------------------------------------ helpers
    def g(self, cls, name, param, default=None):
        return self.ines.get(cls, name, param, default)

    def val(self, value, year, what=None):
        """Scalar value of an INES parameter for a milestone year."""
        if value is None:
            return None
        if isinstance(value, Map):
            d = map_to_dict(value)
            period = self.period_of_year[year]
            if period in d:
                return self.val(d[period], year, what)
            points = self.operating_points(value)
            if points:
                return points[-1][1]
            try:
                series = pd.Series(list(value.values), index=pd.to_datetime([str(i) for i in value.indexes]))
            except (ValueError, TypeError):
                return None
            return self._window_mean(series.astype(float), year, what)
        if isinstance(value, TimeSeries):
            return self._window_mean(ts_to_series(value), year, what)
        if isinstance(value, Array):
            vals = list(value.values)
            if len(vals) == len(self.years):
                return vals[self.years.index(year)]
            return vals[0] if vals else None
        return value

    def operating_points(self, value):
        """Sorted (operating point, value) pairs of an INES map over operating points, else None."""
        if not isinstance(value, Map) or any(str(i) in self.period_of_year.values() for i in value.indexes):
            return None
        try:
            points = sorted((float(i), v) for i, v in zip(value.indexes, value.values))
        except (TypeError, ValueError):
            return None
        if not all(0 <= p <= 1 and is_number(v) for p, v in points):
            return None
        return [(p, float(v)) for p, v in points]

    def _min_operating_point(self, u, a):
        eff = self.g("unit", u, "efficiency")
        if isinstance(eff, Map) and self.operating_points(eff) is None:
            d = map_to_dict(eff)
            eff = d.get(self.period_of_year[self.years[0]], next(iter(d.values()), None))
            if any(self.operating_points(v) for v in d.values() if isinstance(v, Map)):
                self.report.note(f"Unit {u}: minimum operating point taken from the first period (Tulipa has one per asset).")
        points = self.operating_points(eff)
        if not points:
            return
        self.b.set("asset", (a,), min_operating_point=points[0][0])
        if len({round(v, 12) for _, v in points}) > 1:
            self.report.note(f"Unit {u}: efficiency varies with the operating point; Tulipa uses the full-load efficiency {points[-1][1]}.")

    def _window_mean(self, series, year, what):
        start, end = self.windows[year]
        part = series[(series.index >= start) & (series.index < end)]
        if what:
            self.report.note(f"{what}: time series was averaged to a constant because Tulipa needs a constant there.")
        return float(part.mean()) if len(part) else float(series.mean())

    def profile(self, value, year):
        """Profile (numpy array over the Tulipa timesteps of a year) or a scalar."""
        if value is None:
            return None
        if isinstance(value, Map):
            d = map_to_dict(value)
            period = self.period_of_year[year]
            if period in d:
                return self.profile(d[period], year)
            try:
                series = pd.Series(np.asarray(value.values, dtype=float), index=pd.to_datetime([str(i) for i in value.indexes]))
            except (ValueError, TypeError):
                return None
            return self._blocks(series.sort_index(), year)
        if isinstance(value, TimeSeries):
            return self._blocks(ts_to_series(value), year)
        v = self.val(value, year)
        return float(v) if is_number(v) else None

    def _blocks(self, series, year):
        blocks = self.blocks[year]
        start, end = self.windows[year]
        idx = series.index.union(blocks)
        filled = series.reindex(idx).ffill().bfill()
        filled = filled[(filled.index >= start) & (filled.index < end)]
        pos = np.searchsorted(blocks.values, filled.index.values, side="right") - 1
        out = pd.Series(filled.values).groupby(pos).mean()
        return out.reindex(range(len(blocks))).ffill().bfill().to_numpy(dtype=float)

    def add_profile(self, kind, key, ptype, year, arr):
        name = f"{ptype}-{'-'.join(key)}"
        if kind == "asset":
            self.b.set("assets_profiles", (key[0], year, ptype), profile_name=name)
        else:
            self.b.set("flows_profiles", (key[0], key[1], year, ptype), profile_name=name)
        for k, start, nk in self.sections[year]:
            for t in range(1, nk + 1):
                self.b.set("profiles_rep_periods", (name, year, k, t), value=float(arr[start + t - 1]))

    # ------------------------------------------------------------ time
    def build_time(self):
        ines = self.ines
        solves = ines.names("solve_pattern")
        if len(solves) > 1:
            self.report.note(f"Several solve_patterns found, only '{solves[0]}' is used.")
        solve = solves[0] if solves else None
        periods = value_list(self.g("solve_pattern", solve, "period")) if solve else []
        if not periods:
            periods = ines.names("period")
        systems = ines.names("system")
        timeline = None
        for s in systems:
            tl = self.g("system", s, "timeline")
            if isinstance(tl, TimeSeries):
                timeline = ts_to_series(tl).index
                break
        res = duration_hours(self.g("solve_pattern", solve, "time_resolution")) if solve else None
        if res is None and timeline is not None and len(timeline) > 1:
            res = float(pd.Series(timeline).diff().median() / pd.Timedelta(hours=1))
        res = res or 1.0
        sp_start = self.g("solve_pattern", solve, "start_time") if solve else None
        sp_dur = self.g("solve_pattern", solve, "duration") if solve else None
        if not periods:
            if timeline is None:
                raise ValueError("INES data has no periods and no timeline; cannot build the Tulipa time structure.")
            periods = [str(timeline[0].year)]
            self.report.note(f"No INES period found, created milestone year {periods[0]} from the timeline.")

        self.years, self.period_of_year, self.windows, self.blocks, self.res = [], {}, {}, {}, {}
        self.sections, self.rep_names = {}, {}
        self.rep_weighted = False
        for i, p in enumerate(periods):
            start = to_timestamp(self.g("period", p, "start_time"))
            if start is None:
                start = to_timestamp(_pick(sp_start, i, len(periods)))
            m = re.search(r"(\d{4})", p)
            name_year = int(m.group(1)) if m else None
            if start is None:
                if timeline is None:
                    raise ValueError(f"Period {p} has no start_time and there is no timeline.")
                cand = [t for t in timeline if name_year is None or t.year == name_year]
                start = cand[0] if cand else timeline[0]
            dur_value = self.g("period", p, "duration")
            sections_h = [duration_hours(x) for x in dur_value.values] if isinstance(dur_value, Array) else None
            dur = sum(sections_h) if sections_h else duration_hours(dur_value)
            if dur is None:
                dur = duration_hours(_pick(sp_dur, i, len(periods)))
            if dur is None:
                if timeline is None:
                    raise ValueError(f"Period {p} has no duration and there is no timeline.")
                later = [t for t in timeline if t >= start]
                nxt = to_timestamp(self.g("period", periods[i + 1], "start_time")) if i + 1 < len(periods) else None
                if nxt is not None:
                    later = [t for t in later if t < nxt]
                dur = ((later[-1] - start) / pd.Timedelta(hours=1) + res) if later else res
            year = name_year if name_year is not None else start.year
            if year in self.period_of_year:
                year = start.year if start.year not in self.period_of_year else max(self.period_of_year) + 1
                self.report.note(f"Period {p}: milestone year collided, using {year}.")
            n = max(1, int(round(dur / res)))
            self.years.append(year)
            self.period_of_year[year] = p
            self.windows[year] = (start, start + pd.Timedelta(hours=dur))
            self.blocks[year] = pd.date_range(start, periods=n, freq=pd.Timedelta(hours=res))
            self.res[year] = res
            self._rep_periods(solve, p, year, n, res, sections_h)
            self.ines.mark("period", p, "start_time")
            self.ines.mark("period", p, "duration")
        for s in systems:
            self.ines.mark("system", s, "timeline")
        if solve:
            for prm in ("period", "start_time", "duration", "time_resolution"):
                self.ines.mark("solve_pattern", solve, prm)
        if all(len(s) == 1 for s in self.sections.values()):
            self.report.note(
                "Each INES period becomes a Tulipa milestone year with one representative period that covers the whole "
                "period (weight 1); time series are averaged to the INES time_resolution."
            )

    def _rep_periods(self, solve, p, year, n, res, sections_h):
        """Representative periods of a period from (stochastic_)representative_period_mapping; they follow each other in
        the period's timeline in mapping order, with lengths from an Array period.duration (else equal)."""
        weights = {}
        for prm, stochastic in (("stochastic_representative_period_mapping", True), ("representative_period_mapping", False)):
            value = self.g("solve_pattern", solve, prm) if solve else None
            if not isinstance(value, Map) or weights:
                continue
            per_period = map_to_dict(value).get(p)
            if not isinstance(per_period, Map):
                continue
            branches = map_to_dict(per_period) if stochastic else {1: per_period}
            for i, (branch, reps) in enumerate(branches.items(), start=1):
                scen = int(branch) if str(branch).isdigit() else i
                weights[scen] = {str(r): float(w) for r, w in map_to_dict(reps).items()}
        names = []
        for reps in weights.values():
            names += [r for r in reps if r not in names]
        if not names:
            names, weights = [None], {1: {None: 1.0}}
        if sections_h and len(sections_h) == len(names):
            lengths = [max(1, int(round(h / res))) for h in sections_h]
        else:
            lengths = [n // len(names)] * len(names)
            lengths[-1] += n - sum(lengths)
            if len(names) > 1:
                self.report.note(f"Period {p}: {len(names)} representative periods of equal length (no period.duration array).")
        self.sections[year], self.rep_names[year] = [], names
        if names[0] is not None:
            self.rep_weighted = True
        start = 0
        for k, (name, nk) in enumerate(zip(names, lengths), start=1):
            self.sections[year].append((k, start, nk))
            start += nk
            self.b.set("rep_periods_data", (year, k), num_timesteps=nk, resolution=res)
            self.b.set("timeframe_data", (year, k), num_timesteps=nk)
            for scen, reps in weights.items():
                if name in reps:
                    self.b.set("rep_periods_mapping", (year, k, k, scen), weight=reps[name])

    # ------------------------------------------------------------ topology
    def classify(self):
        ines = self.ines
        self.nodes = ines.names("node")
        self.units = ines.names("unit")
        self.node_type = {n: (self.g("node", n, "node_type") or "balance") for n in self.nodes}
        self.inputs = {u: [] for u in self.units}
        self.outputs = {u: [] for u in self.units}
        for n, u in ines.entities.get("node__to_unit", []):
            self.inputs[u].append(n)
        for u, n in ines.entities.get("unit__to_node", []):
            self.outputs[u].append(n)
        self.nln = list(ines.entities.get("node__link__node", []))
        linked_nodes = {x for a, _, b in self.nln for x in (a, b)}

        self.absorbed = set()
        for c in [n for n in self.nodes if self.node_type[n] == COMMODITY]:
            consumers = [u for u in self.units if c in self.inputs[u]]
            fed = any(c in self.outputs[u] for u in self.units)
            if consumers and not fed and c not in linked_nodes and all(
                len(self.inputs[u]) == 1 and self.outputs[u] for u in consumers
            ):
                self.absorbed.add(c)

        self.asset_type = {}
        self.node_asset = {}
        for n in self.nodes:
            nt = self.node_type[n]
            if nt == STORAGE:
                self.asset_type[n] = "storage"
            elif nt == COMMODITY:
                if n in self.absorbed:
                    continue
                outgoing = any(n in self.inputs[u] for u in self.units) or any(a == n for a, _, _ in self.nln)
                self.asset_type[n] = "producer" if outgoing else "consumer"
            else:
                self.asset_type[n] = "consumer"
            self.node_asset[n] = n
        self.unit_asset = {}
        for u in self.units:
            if not self.outputs[u]:
                self.report.skip("unit (without outputs)", [u], "Tulipa conversion assets need at least one output flow.")
                continue
            name = u if u not in self.asset_type else f"{u}_unit"
            if name != u:
                self.report.note(f"Unit {u} has the same name as a node; its Tulipa asset is called {name}.")
            ins = [n for n in self.inputs[u] if n not in self.absorbed]
            self.asset_type[name] = "conversion" if ins else "producer"
            self.unit_asset[u] = name

    def add_flow(self, frm, to, src, transport=False):
        key = (frm, to)
        if key in self.flow_info:
            self.report.skip("duplicate flow", [f"{frm}->{to}"], "Tulipa allows only one flow between two assets in one direction.")
            return None
        self.flow_info[key] = {"src": src, "transport": transport}
        self.b.set("flow", key, is_transport=transport)
        for y in self.years:
            self.b.set("flow_milestone", key + (y,))
            self.b.set("flow_commission", key + (y,))
        return key

    def build_assets(self):
        for a, t in self.asset_type.items():
            self.b.set("asset", (a,), type=t)
            for y in self.years:
                self.b.set("asset_milestone", (a, y))
                self.b.set("asset_commission", (a, y))
                self.b.set("asset_both", (a, y, y))
        for n in self.nodes:
            self.ines.mark("node", n, "node_type")

    # ------------------------------------------------------------ units
    def build_units(self):
        self.main_flow = {}
        self.combined = {}
        for u, a in self.unit_asset.items():
            outs = [("unit__to_node", (u, n)) for n in self.outputs[u]]
            ins = [("node__to_unit", (n, u)) for n in self.inputs[u] if n not in self.absorbed]
            for cls, (uu, n) in outs:
                key = self.add_flow(a, self.node_asset[n], (cls, (uu, n)))
                if key:
                    self.ines_flow_to_tulipa[(cls, (uu, n))] = key
            for cls, (n, uu) in ins:
                key = self.add_flow(self.node_asset[n], a, (cls, (n, uu)))
                if key:
                    self.ines_flow_to_tulipa[(cls, (n, uu))] = key

            main = next((f for f in outs if self.ines.has(*f, "capacity")), None)
            derived_from_input = False
            if main is None:
                main = next((f for f in ins if self.ines.has(*f, "capacity")), None)
                derived_from_input = main is not None
            if main is None:
                main = outs[0]
            self.main_flow[u] = main
            cap = self.g(*main, "capacity")
            if is_number(cap):
                if derived_from_input:
                    eff = self.val(self.g("unit", u, "efficiency"), self.years[0])
                    cap = cap * (eff if is_number(eff) else 1.0)
                    self.report.note(f"Unit {u}: Tulipa limits outputs, capacity derived from the input capacity times efficiency.")
                self.b.set("asset", (a,), capacity=float(cap))
            else:
                self.report.note(f"Unit {u} has no capacity; the Tulipa asset gets capacity 0 (no output) unless invested.")
            for f in ins:
                if f is not main and self.ines.has(*f, "capacity") and self.asset_type.get(f[1][0]) != "storage":
                    self.report.skip(f"{f[0]}.capacity (input flow)", [f[1]], "Tulipa capacity limits only the outputs of producer/conversion assets.")
                    self.ines.mark(*f, "capacity")
            combined = self.g("unit", u, "combined_capacity")
            self.combined[u] = None
            if is_number(combined) and combined > 0 and not derived_from_input:
                if not is_number(cap):
                    cap = float(combined)
                    self.b.set("asset", (a,), capacity=cap)
                # Tulipa: sum(cap/combined * c * output) <= cap  <=>  sum(c * output) <= combined
                self.combined[u] = float(cap) / float(combined)
                for f in outs:
                    key = self.ines_flow_to_tulipa.get(f)
                    c = self.g(*f, "capacity_coefficient")
                    c = float(c) if is_number(c) else 1.0
                    c2 = self.g(*f, "capacity")
                    if is_number(c2) and c > 0 and float(c2) < float(combined) / c - 1e-9:
                        self.report.note(f"Unit {u}: capacity {c2} of {f[0]} {'/'.join(f[1])} is below its share of combined_capacity {combined}; Tulipa has only the combined limit.")
                    if key:
                        for y in self.years:
                            self.b.set("flow_commission", key + (y,), capacity_coefficient=self.combined[u] * c)
            elif is_number(combined):
                self.report.skip("unit.combined_capacity", [u], "Not mapped when the Tulipa capacity is derived from an input flow.")
            for f in outs + ins:
                if self.combined[u] is None and self.ines.has(*f, "capacity_coefficient"):
                    self.ines.mark(*f, "capacity_coefficient")
                    self.report.skip(f"{f[0]}.capacity_coefficient", [f[1]], "Used only with unit.combined_capacity (input flows have no Tulipa capacity coefficient).")
                elif f in ins and self.ines.has(*f, "capacity_coefficient"):
                    self.ines.mark(*f, "capacity_coefficient")
                    self.report.skip(f"{f[0]}.capacity_coefficient", [f[1]], "Tulipa capacity limits only the outputs of producer/conversion assets.")
            for f in outs:
                if f is main or self.combined[u] is not None:
                    continue
                key = self.ines_flow_to_tulipa.get(f)
                c2 = self.g(*f, "capacity")
                if is_number(cap) and is_number(c2) and c2 > 0:
                    coef = float(cap) / float(c2)
                else:
                    coef = 1.0 if derived_from_input else 0.0
                if coef and not derived_from_input:
                    self.report.note(f"Unit {u}: several output capacities combined into one Tulipa limit (sum of outputs scaled by capacity_coefficient).")
                if key:
                    for y in self.years:
                        self.b.set("flow_commission", key + (y,), capacity_coefficient=coef)

            self._existing_and_investments(
                "unit", u, a, "units_existing", "investment_method", "retirement_method",
                "units_invest_max_period", "units_invest_min_period",
                "units_max_cumulative", "units_min_cumulative", cap if is_number(cap) else None,
            )
            self._unit_commitment(u, a)
            self._min_operating_point(u, a)
            self._ramping(u, a, main)
            self._availability(u, a, outs)
            self._commodity(u, a, outs)
            self.ines.mark("unit", u, "conversion_method")
            for f in outs + ins:
                self.ines.mark(*f, "profile_method")

    def _existing_and_investments(self, cls, ent, a, existing, inv_m, ret_m, inv_max, inv_min, max_cum, min_cum, cap, energy=False):
        b = self.b
        exist = self.g(cls, ent, existing)
        im = self.g(cls, ent, inv_m)
        rm = self.g(cls, ent, ret_m)
        investable = im not in (None, "not_allowed")
        decom = rm not in (None, "not_retired")
        cy, dy = self._dates(cls, ent, "storage_" if energy else "")
        for y in self.years:
            e = self.val(exist, y)
            col = "initial_storage_units" if energy else "initial_units"
            if is_number(e):
                c, alive = self._vintage(y, cy, dy)
                b.set("asset_both", (a, y, c), **{col: float(e) if alive else 0.0})
                if c != y:
                    b.set("asset_commission", (a, c))
                    if decom:
                        b.set("asset_both", (a, y, c), decommissionable=True)
            if decom:
                b.set("asset_both", (a, y, y), decommissionable=True)
            if investable:
                b.set("asset_milestone", (a, y), investable=True)
            sfx = "_storage_energy" if energy else ""
            for prm, colname in ((inv_max, "investment_max_limit" + sfx), (inv_min, "investment_min_limit" + sfx)):
                v = self.val(self.g(cls, ent, prm), y)
                if is_number(v) and prm == inv_max and investable and float(v) == 0:
                    b.set("asset_milestone", (a, y), investable=False)
                    continue
                if is_number(v) and float(v) >= self.big:
                    continue
                if is_number(v):
                    if cap is None:
                        self.report.skip(f"{cls}.{prm}", [ent], "Needs a capacity to convert units to MW in Tulipa.")
                    else:
                        b.set("asset_commission", (a, y), **{colname: float(v) * cap})
            if not energy:
                for prm, colname in ((max_cum, "max_available_units"), (min_cum, "min_available_units")):
                    v = self.val(self.g(cls, ent, prm), y)
                    if is_number(v):
                        b.set("asset_milestone", (a, y), **{colname: float(v)})
        if im not in (None, "not_allowed", "no_limits"):
            self.report.note(f"{cls} {ent}: investment_method '{im}' mapped to Tulipa investment limits per milestone year.")

    def _dates(self, cls, ent, prefix=""):
        """Years of the INES (storage_)commission_date and (storage_)decommission_date of existing assets."""
        years = []
        for prm in (prefix + "commission_date", prefix + "decommission_date"):
            ts = to_timestamp(self.g(cls, ent, prm))
            if ts is not None and (ts.month, ts.day, ts.hour, ts.minute) != (1, 1, 0, 0):
                self.report.note(f"{cls} {ent}: {prm} {ts} rounded to the year {ts.year} (Tulipa vintages are years).")
            years.append(None if ts is None else ts.year)
        return years

    def _vintage(self, y, cy, dy):
        """Row commission year (Tulipa aggregated method needs it equal to y) and whether existing units exist in y."""
        if dy is not None and y >= dy:
            return y, False
        if cy is not None and cy > y:
            return y, False
        return y, True

    def _partition(self, value, y, what):
        """Tulipa (specification, partition) for an INES time_resolution (Duration or Array of Durations), None if model resolution."""
        if value is None:
            return None
        durations = list(value.values) if isinstance(value, Array) else [value]
        blocks = []
        for d in durations:
            hours = duration_hours(d)
            k = hours / self.res[y]
            if k < 1 - 1e-9 or abs(k - round(k)) > 1e-9:
                self.report.note(f"{what}: time_resolution {hours} h is not a multiple of the model resolution {self.res[y]} h; rounded.")
            blocks.append(max(1, int(round(k))))
        if len(set(blocks)) == 1:
            return None if blocks[0] == 1 else ("uniform", str(blocks[0]))
        n = len(self.blocks[y])
        if sum(blocks) != n:
            self.report.note(f"{what}: time_resolution blocks cover {sum(blocks)} of {n} timesteps in {y}.")
        return "math", blocks_to_math(blocks)

    def build_partitions(self):
        def put(table, key, p):
            """Partition p of a whole period, cut into the representative periods of the year key[-1]."""
            if not p:
                return
            y = key[-1]
            secs = self.sections[y]
            if len(secs) == 1:
                self.b.set(table, key + (secs[0][0],), specification=p[0], partition=p[1])
                return
            blocks = partition_blocks(*p)
            if p[0] == "uniform":
                blocks = blocks * -(-len(self.blocks[y]) // blocks[0])
            edges = np.cumsum([0] + blocks)
            for k, s, nk in secs:
                parts = [int(min(b1, s + nk) - max(b0, s)) for b0, b1 in zip(edges[:-1], edges[1:]) if min(b1, s + nk) > max(b0, s)]
                if len(set(parts)) == 1:
                    if parts[0] > 1:
                        self.b.set(table, key + (k,), specification="uniform", partition=str(parts[0]))
                elif parts:
                    self.b.set(table, key + (k,), specification="math", partition=blocks_to_math(parts))

        for u, a in self.unit_asset.items():
            value = self.g("unit", u, "time_resolution")
            flows = [(f, k) for f, k in self.ines_flow_to_tulipa.items() if u == (f[1][0] if f[0] == "unit__to_node" else f[1][1])]
            for y in self.years:
                p = self._partition(value, y, f"unit {u}")
                put("assets_rep_periods_partitions", (a, y), p)
                for f, key in flows:
                    # a flow time_resolution overrides the unit's
                    fv = self.g(*f, "time_resolution")
                    put("flows_rep_periods_partitions", key + (y,), self._partition(fv, y, f"{f[0]} {'/'.join(f[1])}") if fv is not None else p)
        for n, a in self.node_asset.items():
            value = self.g("node", n, "time_resolution")
            for y in self.years:
                put("assets_rep_periods_partitions", (a, y), self._partition(value, y, f"node {n}"))
        for key, info in self.flow_info.items():
            if info["src"][0] != "link":
                continue
            l = info["src"][1][0]
            for y in self.years:
                put("flows_rep_periods_partitions", key + (y,), self._partition(self.g("link", l, "time_resolution"), y, f"link {l}"))

    def _unit_commitment(self, u, a):
        sm = self.g("unit", u, "startup_method")
        if sm in (None, "no_startup"):
            return
        uc = self.mapping["startup_method_to_unit_commitment"].get(sm, "basic")
        if self.ines.has("unit", u, "min_uptime") or self.ines.has("unit", u, "min_downtime"):
            uc = "3var"
        self.b.set("asset", (a,), unit_commitment=uc, unit_commitment_integer=sm.startswith("integer"))
        if "tiers" in sm or "trajectory" in sm:
            self.report.note(f"Unit {u}: startup_method '{sm}' approximated with Tulipa unit commitment '{uc}'.")

    def _ramping(self, u, a, main):
        rm = self.g(*main, "ramp_method")
        if rm in ("ramp_limit", "ramp_limit_and_cost") or self.ines.has(*main, "ramp_limit_up") or self.ines.has(*main, "ramp_limit_down"):
            self.b.set("asset", (a,), ramping=True)
        if rm in ("ramp_cost", "ramp_limit_and_cost"):
            self.report.note(f"Unit {u}: ramp costs are not supported by Tulipa.")

    def _availability(self, u, a, outs):
        avail = self.g("unit", u, "availability")
        upper, upper_rp = None, None
        for f in outs:
            pm = self.g(*f, "profile_method")
            if pm not in ("upper_limit", "upper_and_lower_limit"):
                continue
            for prm in ("profile_limit_upper", "profile_limit_upper_representative_period"):
                if not self.ines.has(*f, prm):
                    continue
                if (upper if prm == "profile_limit_upper" else upper_rp) is not None:
                    self.report.skip(f"{f[0]}.{prm} (secondary flow)", [f[1]], "Tulipa availability is per asset; only one profile is used.")
                    self.ines.mark(*f, prm)
                elif prm == "profile_limit_upper":
                    upper = self.g(*f, prm)
                else:
                    upper_rp = self.g(*f, prm)
        if avail is None and upper is None and upper_rp is None:
            return
        for y in self.years:
            p1 = self.profile(avail, y)
            p2 = self.profile(upper, y)
            p3 = self._rep_profile(upper_rp, y, f"unit {u}")
            prof = (1.0 if p1 is None else p1) * (1.0 if p2 is None else p2) * (1.0 if p3 is None else p3)
            arr = np.broadcast_to(np.asarray(prof, dtype=float), (len(self.blocks[y]),))
            self.add_profile("asset", (a,), "availability", y, arr)

    def _rep_profile(self, value, y, what):
        """Array over the year's timesteps from a 2D map representative period -> timestep -> value."""
        if not isinstance(value, Map):
            return None
        by_rep = map_to_dict(value)
        arr = np.ones(len(self.blocks[y]))
        for (k, s, nk), name in zip(self.sections[y], self.rep_names[y]):
            inner = by_rep.get(name) if name is not None else next(iter(by_rep.values()), None)
            if not isinstance(inner, Map):
                continue
            vals = [float(v) for v in inner.values]
            if len(vals) != nk:
                self.report.note(f"{what}: representative period {name} has {len(vals)} values for {nk} timesteps; padded/cut.")
                vals = (vals + [vals[-1]] * nk)[:nk] if vals else [1.0] * nk
            arr[s:s + nk] = vals
        return arr

    def _commodity(self, u, a, outs):
        absorbed_in = [n for n in self.inputs[u] if n in self.absorbed]
        if not absorbed_in:
            return
        c = absorbed_in[0]
        price = self.g("node", c, "commodity_price")
        in_cost = self.g("node__to_unit", (c, u), "other_operational_cost")
        eff = self.g("unit", u, "efficiency")
        for f in outs:
            key = self.ines_flow_to_tulipa.get(f)
            if not key:
                continue
            for y in self.years:
                e = self.val(eff, y, f"unit {u} efficiency")
                if is_number(e):
                    self.b.set("flow_commission", key + (y,), producer_efficiency=float(e))
                p = self.profile(price, y)
                ic = self.val(in_cost, y, f"node__to_unit {c}/{u} other_operational_cost")
                if p is None and ic is None:
                    continue
                p = (0.0 if p is None else p) + (float(ic) if is_number(ic) else 0.0)
                if np.ndim(p) == 0:
                    self.b.set("flow_milestone", key + (y,), commodity_price=float(p))
                else:
                    base = float(np.max(np.abs(p))) or 1.0
                    self.b.set("flow_milestone", key + (y,), commodity_price=base)
                    self.add_profile("flow", key, "commodity_price", y, p / base)
        self.ines.mark("node", c, "node_type")
        for prm in ("investment_cost", "fixed_cost"):
            if self.ines.has("node__to_unit", (c, u), prm):
                self.report.skip(f"node__to_unit.{prm} (commodity input)", [(c, u)], "Input flow from a commodity is not a Tulipa flow.")

    # ------------------------------------------------------------ nodes
    def build_nodes(self):
        for n in self.nodes:
            t = self.asset_type.get(n)
            if t is None:
                continue
            if self.node_type[n] == COMMODITY:
                self._commodity_node(n, t)
            elif t == "consumer":
                self._consumer(n)
            elif t == "storage":
                self._storage(n)

    def _scaled_flow_profile(self, n, y):
        fp = self.g("node", n, "flow_profile")
        method = self.g("node", n, "flow_scaling_method")
        if fp is None or method == "no_inflow":
            return None
        prof = self.profile(fp, y)
        if prof is None:
            return None
        if method == "scale_to_annual":
            annual = self.val(self.g("node", n, "flow_annual"), y)
            if is_number(annual):
                arr = np.broadcast_to(np.asarray(prof, dtype=float), (len(self.blocks[y]),))
                total = abs(float(np.sum(arr)) * self.res[y])
                hours = len(arr) * self.res[y]
                if total > 0:
                    prof = arr * abs(annual) * hours / 8760.0 / total
        return prof

    def _consumer(self, n):
        b = self.b
        for y in self.years:
            prof = self._scaled_flow_profile(n, y)
            if prof is None:
                continue
            demand = -np.asarray(prof, dtype=float)
            if np.any(demand < -1e-12):
                self.report.skip("node.flow_profile (positive inflow)", [n], "Tulipa consumers only have a non-negative demand.")
                demand = np.clip(demand, 0, None)
            peak = float(np.max(demand)) if demand.size else 0.0
            b.set("asset_milestone", (n, y), peak_demand=peak)
            if demand.ndim and peak > 0:
                self.add_profile("asset", (n,), "demand", y, demand / peak)
        for prm in ("flow_scaling_method", "flow_annual"):
            self.ines.mark("node", n, prm)
        self._penalties(n)

    def _penalties(self, n):
        up = self.g("node", n, "penalty_upward")
        down = self.g("node", n, "penalty_downward")
        y0 = self.years[0]
        upv = self.val(up, y0, f"node {n} penalty_upward")
        downv = self.val(down, y0, f"node {n} penalty_downward")
        sense = "=="
        if is_number(downv) and downv == 0:
            sense = ">="
        elif is_number(upv) and upv == 0:
            sense = "<="
        if sense != "==":
            self.b.set("asset", (n,), consumer_balance_sense=sense)
        if is_number(upv) and upv > 0:
            pa = n + self.settings.get("penalty_upward_suffix", "_penalty_upward")
            self._slack_asset(pa, "producer", (pa, n), up, n)
        if is_number(downv) and downv > 0:
            pa = n + self.settings.get("penalty_downward_suffix", "_penalty_downward")
            self._slack_asset(pa, "consumer", (n, pa), down, n)
            self.b.set("asset", (pa,), consumer_balance_sense=">=")

    def _slack_asset(self, pa, typ, key, cost, n):
        self.asset_type[pa] = typ
        self.b.set("asset", (pa,), type=typ, capacity=self.big if typ == "producer" else 0.0)
        for y in self.years:
            self.b.set("asset_milestone", (pa, y))
            self.b.set("asset_commission", (pa, y))
            self.b.set("asset_both", (pa, y, y), initial_units=1.0)
        self.add_flow(key[0], key[1], ("node", (n,)))
        for y in self.years:
            self.b.set("flow_milestone", key + (y,), operational_cost=float(self.val(cost, y, f"node {n} penalty")))

    def _commodity_node(self, n, t):
        price = self.g("node", n, "commodity_price")
        if t == "producer":
            self.b.set("asset", (n,), capacity=self.big)
            for y in self.years:
                self.b.set("asset_both", (n, y, y), initial_units=1.0)
            self.report.note(f"Commodity node {n} became a Tulipa producer with unlimited capacity.")
        else:
            self.b.set("asset", (n,), consumer_balance_sense=">=")
        if price is None:
            return
        self._pending_commodity_price.append((n, t, price))

    def apply_commodity_prices(self):
        for n, t, price in self._pending_commodity_price:
            flows = [k for k in self.flow_info if (k[0] == n if t == "producer" else k[1] == n)]
            for key in flows:
                for y in self.years:
                    p = self.profile(price, y)
                    if p is None:
                        continue
                    if t == "producer":
                        if np.ndim(p) == 0:
                            self.b.set("flow_milestone", key + (y,), commodity_price=float(p))
                        else:
                            base = float(np.max(np.abs(p))) or 1.0
                            self.b.set("flow_milestone", key + (y,), commodity_price=base)
                            self.add_profile("flow", key, "commodity_price", y, p / base)
                    else:
                        old = self.b.get("flow_milestone", key + (y,), "operational_cost", 0.0)
                        self.b.set("flow_milestone", key + (y,), operational_cost=old + float(np.mean(p)))
                        if np.ndim(p):
                            self.report.note(f"Commodity price of sink node {n} averaged to a constant operational cost.")

    def _storage(self, n):
        b = self.b
        cap_e = self.g("node", n, "storage_capacity")
        self._existing_and_investments(
            "node", n, n, "storages_existing", "storage_investment_method", "storage_retirement_method",
            "storages_invest_max_period", "storages_invest_min_period", None, None,
            float(cap_e) if is_number(cap_e) else None, energy=True,
        )
        sim = self.g("node", n, "storage_investment_method")
        if sim not in (None, "not_allowed"):
            b.set("asset", (n,), storage_method_energy="optimize_storage_capacity")
        if self.rep_weighted:
            # storages link their state across the represented periods by default
            b.set("asset", (n,), use_inter_period_constraints=True)
        fix_m = self.g("node", n, "storage_state_fix_method")
        if fix_m in ("fix_start", "fix_start_and_horizon_end"):
            for y in self.years:
                v = self.val(self.g("node", n, "storage_state_fix"), y, f"node {n} storage_state_fix")
                if is_number(v):
                    b.set("asset_milestone", (n, y), initial_storage_level=float(v))
            if fix_m == "fix_start_and_horizon_end":
                self.report.note(f"Storage {n}: Tulipa forces the final level to be at least the initial level.")
        elif fix_m not in (None, "none"):
            self.report.skip("node.storage_state_fix_method", [n], f"Only fix_start can be represented in Tulipa (got {fix_m}).")
        self.ines.mark("node", n, "storage_state_fix_method")
        self.ines.mark("node", n, "storage_limit_method")
        for prm, ptype in (("storage_state_upper_limit", "max_storage_level"), ("storage_state_lower_limit", "min_storage_level")):
            v = self.g("node", n, prm)
            if v is None:
                continue
            for y in self.years:
                p = self.profile(v, y)
                arr = np.broadcast_to(np.asarray(p, dtype=float), (len(self.blocks[y]),))
                self.add_profile("asset", (n,), ptype, y, arr)
        for y in self.years:
            prof = self._scaled_flow_profile(n, y)
            if prof is None:
                continue
            arr = np.broadcast_to(np.asarray(prof, dtype=float), (len(self.blocks[y]),))
            if np.any(arr < -1e-12):
                self.report.skip("node.flow_profile (storage outflow)", [n], "Tulipa storage inflows must be non-negative.")
                arr = np.clip(arr, 0, None)
            peak = float(np.max(arr))
            b.set("asset_milestone", (n, y), storage_inflows=peak)
            if peak > 0:
                self.add_profile("asset", (n,), "inflows", y, arr / peak)
        for prm in ("flow_scaling_method", "flow_annual"):
            self.ines.mark("node", n, prm)

    # ------------------------------------------------------------ links
    def build_links(self):
        storage_links = {}
        for a, l, bb in self.nln:
            if a not in self.node_asset or bb not in self.node_asset:
                self.report.skip("link (to absorbed commodity)", [l], "Link endpoint is not a Tulipa asset.")
                continue
            method = self.g("link", l, "transfer_method")
            one_way = method == "one_way"
            sa, sb = self.asset_type[a] == "storage", self.asset_type[bb] == "storage"
            if one_way or sa or sb:
                k1 = self.add_flow(a, bb, ("link", (l,)))
                k2 = None if one_way else self.add_flow(bb, a, ("link", (l,)))
                for k in (k1, k2):
                    if k:
                        self._link_costs(a, l, bb, k)
                if sa ^ sb:
                    s, other = (a, bb) if sa else (bb, a)
                    storage_links.setdefault(s, []).append((a, l, bb, other))
                elif one_way:
                    self.report.note(f"Link {l} is one_way: Tulipa flow {a}->{bb} is not a transport flow and has no capacity limit.")
                else:
                    self.report.note(f"Link {l} connects two storages; Tulipa flows have no capacity limit there.")
                self.ines.mark("link", l, "transfer_method")
                continue
            key = self.add_flow(a, bb, ("link", (l,)), transport=True)
            if key is None:
                continue
            self.ines.mark("link", l, "transfer_method")
            self._transport(a, l, bb, key)
        for a, l, bb in self.nln:
            carrier = self.g("node__link__node", (a, l, bb), "carrier")
            if carrier is None:
                continue
            for key in ((a, bb), (bb, a)):
                if self.flow_info.get(key, {}).get("src") == ("link", (l,)):
                    self.b.set("flow", key, carrier=str(carrier))
        for s in self.nodes:
            if self.asset_type.get(s) == "storage":
                self._storage_power(s, storage_links.get(s, []))
                self._energy_to_power(s, storage_links.get(s, []))

    def _link_costs(self, a, l, bb, key):
        oc = self.g("node__link__node", (a, l, bb), "operational_cost") if key == (a, bb) else None
        if oc is None:
            oc = self.g("link", l, "operational_cost")
        for y in self.years:
            v = self.val(oc, y, f"link {l} operational_cost")
            if is_number(v):
                self.b.set("flow_milestone", key + (y,), operational_cost=float(v))

    def _link_capacity(self, a, l, bb):
        cap = self.g("link", l, "capacity")
        nl = self.g("node__link__node", (a, l, bb), "capacity")
        if cap is None:
            cap = nl
        elif nl is not None and nl != cap:
            self.report.skip("node__link__node.capacity", [(a, l, bb)], "Tulipa transport flows have one capacity for both directions; link capacity used.")
        return cap

    def _transport(self, a, l, bb, key):
        b = self.b
        cap = self._link_capacity(a, l, bb)
        if is_number(cap):
            b.set("flow", key, capacity=float(cap))
        self.ines.mark("link", l, "capacity")
        if self.ines.values.get(("link", (l,), "transfer_method")) == "exact_integer":
            self.report.note(f"Link {l}: exact_integer has no Tulipa counterpart for transport flows (only for storage links, as use_binary_storage_method).")
        exist = self.g("link", l, "links_existing")
        im = self.g("link", l, "investment_method")
        rm = self.g("link", l, "retirement_method")
        cy, dy = self._dates("link", l)
        for y in self.years:
            e = self.val(exist, y)
            row = {}
            c, alive = self._vintage(y, cy, dy)
            if is_number(e):
                units = float(e) if alive else 0.0
                row.update(initial_export_units=units, initial_import_units=units)
            if rm not in (None, "not_retired"):
                row["decommissionable"] = True
            b.set("flow_both", key + (y, c), **row)
            if c != y:
                b.set("flow_commission", key + (c,))
            if im not in (None, "not_allowed"):
                b.set("flow_milestone", key + (y,), investable=True)
            for prm, col in (("links_invest_max_period", "investment_max_limit"), ("links_invest_min_period", "investment_min_limit")):
                v = self.val(self.g("link", l, prm), y)
                if is_number(v) and is_number(cap):
                    b.set("flow_commission", key + (y,), **{col: float(v) * cap})
            av = self.g("link", l, "availability")
            if av is not None:
                p = self.profile(av, y)
                arr = np.broadcast_to(np.asarray(p, dtype=float), (len(self.blocks[y]),))
                self.add_profile("flow", key, "availability", y, arr)
        nl_cost = self.g("node__link__node", (a, l, bb), "operational_cost")
        if nl_cost is not None:
            for y in self.years:
                v = self.val(nl_cost, y, f"node__link__node {a}/{l}/{bb} operational_cost")
                if is_number(v):
                    b.set("flow_milestone", key + (y,), operational_cost=float(v))
        eff = self.ines.values.get(("link", (l,), "efficiency"))
        if is_number(eff) and eff == 1:
            self.ines.mark("link", l, "efficiency")

    def _energy_to_power(self, s, links):
        """INES constraint link_coef * links + storage_coef * storages = 0 gives a fixed energy-to-power ratio."""
        values = self.ines.values
        st = values.get(("node", (s,), "constraint_storage_count_coefficient"))
        if not isinstance(st, Map):
            return
        st = map_to_dict(st)
        for a, l, bb, _ in links:
            lk = values.get(("link", (l,), "constraint_link_count_coefficient"))
            if not isinstance(lk, Map):
                continue
            lk = map_to_dict(lk)
            for c in st:
                if c not in lk or not is_number(st[c]) or not is_number(lk[c]) or st[c] == 0:
                    continue
                sense = values.get(("constraint", (c,), "sense"))
                const = values.get(("constraint", (c,), "constant"))
                if sense not in (None, "equal") or (is_number(const) and const != 0):
                    continue
                cap_e = values.get(("node", (s,), "storage_capacity"))
                cap_p = self._link_capacity(a, l, bb)
                if not (is_number(cap_e) and is_number(cap_p) and cap_p):
                    continue
                # storage units per link unit, converted to MWh of storage per MW of link
                e2p = -float(lk[c]) / float(st[c]) * float(cap_e) / float(cap_p)
                self.b.set("asset", (s,), energy_to_power_ratio=e2p, storage_method_energy="use_fixed_energy_to_power_ratio")
                self.ines.mark("node", s, "constraint_storage_count_coefficient")
                self.ines.mark("link", l, "constraint_link_count_coefficient")
                self.ines.mark("constraint", c, "sense")
                self.ines.mark("constraint", c, "constant")
                if len(st) > 1 or len(lk) > 1:
                    self.report.note(f"Storage {s}: only constraint {c} is used as the energy-to-power ratio; other constraint coefficients are ignored.")
                if self.ines.has("node", s, "storage_investment_cost"):
                    self.report.note(f"Storage {s}: with a fixed energy-to-power ratio Tulipa invests in storage energy through the power investment; storage_investment_cost is not used.")
                return

    def _storage_power(self, s, links):
        b = self.b
        if any(self.ines.values.get(("link", (l,), "transfer_method")) == "exact_integer" for _, l, _, _ in links):
            b.set("asset", (s,), use_binary_storage_method="binary")
            self._pending_binary_limits.append(s)
        if not links:
            b.set("asset", (s,), capacity=self.big)
            for y in self.years:
                b.set("asset_both", (s, y, y), initial_units=1.0)
                if self.b.get("asset_milestone", (s, y), "investable"):
                    b.set("asset_commission", (s, y), investment_max_limit=0.0)
            return
        energy_investable = any(b.get("asset_milestone", (s, y), "investable") for y in self.years)
        caps, existing = [], {y: [] for y in self.years}
        for a, l, bb, _ in links:
            cap = self._link_capacity(a, l, bb)
            self.ines.mark("link", l, "capacity")
            cap = float(cap) if is_number(cap) else self.big
            caps.append(cap)
            link_investable = self.g("link", l, "investment_method") not in (None, "not_allowed")
            for y in self.years:
                e = self.val(self.g("link", l, "links_existing"), y)
                existing[y].append(float(e) if is_number(e) else (0.0 if link_investable else 1.0))
            # left-to-right efficiency may come from node__link__node, the other direction from link
            nl_eff = self.g("node__link__node", (a, l, bb), "efficiency")
            l_eff = self.g("link", l, "efficiency")
            eff_in = nl_eff if bb == s and nl_eff is not None else l_eff
            eff_out = nl_eff if a == s and nl_eff is not None else l_eff
            for y in self.years:
                ein, eout = self.val(eff_in, y, f"link {l} efficiency"), self.val(eff_out, y, f"link {l} efficiency")
                if is_number(ein):
                    b.set("asset_commission", (s, y), storage_charging_efficiency=float(ein))
                if is_number(eout):
                    b.set("asset_commission", (s, y), storage_discharging_efficiency=float(eout))
            if len(links) > 1:
                for prm in ("investment_cost", "fixed_cost", "investment_method", "retirement_method", "links_invest_max_period", "links_invest_min_period"):
                    if self.ines.has("link", l, prm):
                        self.ines.mark("link", l, prm)
                        self.report.skip(f"link.{prm}", [l], "Storage connected by several links: link investments cannot be mapped to the single Tulipa storage power capacity.")
                continue
            for y in self.years:
                if link_investable:
                    b.set("asset_milestone", (s, y), investable=True)
                for prm in ("investment_cost", "fixed_cost"):
                    v = self.val(self.g("link", l, prm), y, f"link {l} {prm}")
                    if is_number(v):
                        b.set("asset_commission", (s, y), **{prm: float(v)})
                for prm, col in (("links_invest_max_period", "investment_max_limit"), ("links_invest_min_period", "investment_min_limit")):
                    v = self.val(self.g("link", l, prm), y)
                    if is_number(v):
                        b.set("asset_commission", (s, y), **{col: float(v) * cap})
                if self.g("link", l, "retirement_method") not in (None, "not_retired"):
                    b.set("asset_both", (s, y, y), decommissionable=True)
            v = self.g("link", l, "investment_uses_integer")
            if v is not None:
                b.set("asset", (s,), investment_integer=bool(v))
            if energy_investable and not link_investable:
                for y in self.years:
                    b.set("asset_commission", (s, y), investment_max_limit=0.0)
        if len(links) == 1:
            b.set("asset", (s,), capacity=caps[0])
            cy, dy = self._dates("link", links[0][1])
            for y in self.years:
                c, alive = self._vintage(y, cy, dy)
                b.set("asset_both", (s, y, c), initial_units=existing[y][0] if alive else 0.0)
                if c != y:
                    b.set("asset_commission", (s, c))
        else:
            self.report.note(f"Storage {s} has several links; their existing capacities are summed into one Tulipa storage power capacity (link investments not mapped).")
            total = sum(c * e for c, e in zip(caps, existing[self.years[0]]))
            b.set("asset", (s,), capacity=total)
            for y in self.years:
                cap_y = sum(c * e for c, e in zip(caps, existing[y]))
                b.set("asset_both", (s, y, y), initial_units=cap_y / total if total else 0.0)

    def _binary_storage_limits(self):
        # Tulipa uses investment_max_limit as big-M for investable binary storages
        for s in self._pending_binary_limits:
            for y in self.years:
                if self.b.get("asset_milestone", (s, y), "investable") and self.b.get("asset_commission", (s, y), "investment_max_limit") is None:
                    self.b.set("asset_commission", (s, y), investment_max_limit=self.big)
                    self.report.note(f"Storage {s}: binary storage is investable without an investment limit; investment_max_limit set to {self.big} (Tulipa needs it).")

    # ------------------------------------------------------------ direct parameters
    def apply_direct(self):
        targets = {"unit": [], "unit_main": [], "unit_flow": [], "storage": [], "link": [], "system": []}
        for u, a in self.unit_asset.items():
            targets["unit"].append((("unit", (u,)), (a,)))
            targets["unit_main"].append((self.main_flow[u], (a,)))
        for ines_flow, key in self.ines_flow_to_tulipa.items():
            targets["unit_flow"].append((ines_flow, key))
        for n in self.nodes:
            if self.asset_type.get(n) == "storage":
                targets["storage"].append((("node", (n,)), (n,)))
        for key, info in self.flow_info.items():
            if info["transport"]:
                targets["link"].append((info["src"], key))
        for s in self.ines.names("system")[:1]:
            targets["system"].append((("system", (s,)), ()))
        for entry in self.mapping["direct_parameters"]:
            for src, dest in targets[entry["role"]]:
                if entry["role"] == "unit" and entry["ines"] == "efficiency" and self.asset_type[dest[0]] == "producer":
                    if not [n for n in self.inputs[src[1][0]] if n in self.absorbed]:
                        self.ines.mark(*src, "efficiency")
                    continue
                value = self.g(src[0], src[1], entry["ines"])
                if value is None:
                    continue
                factor = float(entry.get("factor", 1))
                for table, col in entry["tulipa"]:
                    if table in ("asset", "flow", "model_parameters"):
                        rows = [(dest, self.years[0])]
                        if isinstance(value, (Map, TimeSeries, Array)):
                            self.report.note(f"{src[0]}.{entry['ines']}: period/time varying value reduced to the first period for Tulipa {table}.{col}.")
                    else:
                        rows = [(dest + (y,), y) for y in self.years]
                    for key, y in rows:
                        v = self.val(value, y, f"{src[0]}.{entry['ines']}")
                        if v is None:
                            continue
                        if is_number(v):
                            v = float(v) * factor
                            if self.schema[table][col]["type"] == "INTEGER":
                                v = ceil_int(v)
                        self.b.set(table, key, **{col: v})
        dy = None
        for s in self.ines.names("system")[:1]:
            dy = self.g("system", s, "discount_year")
        self.b.set("model_parameters", (), discount_year=int(round(float(dy))) if is_number(dy) else min(self.years))

    def _storage_unit_flows(self):
        """Tulipa applies storage efficiency and power capacity also to flows between a storage and a unit.
        INES measures the flow at the storage: Tulipa flow F = G / k, k = charging efficiency or 1 / discharging efficiency."""
        b = self.b
        y0 = self.years[0]
        storage_limits = defaultdict(lambda: {"in": 0.0, "out": 0.0})
        for ines_flow, key in self.ines_flow_to_tulipa.items():
            cls, (x1, x2) = ines_flow
            charging = cls == "unit__to_node"
            u, s = (x1, x2) if charging else (x2, x1)
            if self.asset_type.get(s) != "storage":
                continue
            a = self.unit_asset[u]
            col = "storage_charging_efficiency" if charging else "storage_discharging_efficiency"
            eff = {y: float(b.get("asset_commission", (s, y), col) or 1.0) for y in self.years}
            k = {y: e if charging else 1.0 / e for y, e in eff.items()}
            main = self.main_flow[u] == ines_flow
            outs = [("unit__to_node", (u, n)) for n in self.outputs[u]]
            combined = self.combined.get(u) if charging else None
            # a secondary flow capacity becomes a limit of the storage power (Tulipa limits all storage flows by it)
            c2 = self.g(*ines_flow, "capacity")
            if not main and is_number(c2):
                self.ines.mark(*ines_flow, "capacity")
                n_u = b.get("asset_both", (a, y0, y0), "initial_units") or 0.0
                storage_limits[s]["in" if charging else "out"] += float(c2) / k[y0] * float(n_u)
                if charging and combined is None:
                    for y in self.years:
                        b.set("flow_commission", key + (y,), capacity_coefficient=1.0)
            if all(e == 1 for e in eff.values()):
                continue
            if combined is not None:
                # INES combined_capacity counts the flow at the storage side: G = k * F
                for y in self.years:
                    c = b.get("flow_commission", key + (y,), "capacity_coefficient")
                    b.set("flow_commission", key + (y,), capacity_coefficient=float(c if is_number(c) else combined) * k[y0])
            elif charging and main:
                cap = b.get("asset", (a,), "capacity")
                if is_number(cap):
                    b.set("asset", (a,), capacity=float(cap) / k[y0])
                for f in outs:
                    fk = self.ines_flow_to_tulipa.get(f)
                    if f != ines_flow and fk:
                        for y in self.years:
                            c = b.get("flow_commission", fk + (y,), "capacity_coefficient")
                            if is_number(c):
                                b.set("flow_commission", fk + (y,), capacity_coefficient=float(c) / k[y0])
            elif main:
                self.report.note(f"Unit {u}: capacity is on the input from storage {s}; the storage discharging efficiency is not applied to it.")
            side = outs if charging else [n for n in self.inputs[u] if n not in self.absorbed]
            for y in self.years:
                if len(side) == 1 and self.asset_type[a] == "conversion":
                    e = b.get("asset_commission", (a, y), "conversion_efficiency")
                    b.set("asset_commission", (a, y), conversion_efficiency=float(e if is_number(e) else 1.0) / eff[y])
                elif len(side) == 1:
                    e = b.get("flow_commission", key + (y,), "producer_efficiency")
                    if is_number(e):
                        b.set("flow_commission", key + (y,), producer_efficiency=float(e) / eff[y])
                else:
                    c = b.get("flow_commission", key + (y,), "conversion_coefficient")
                    b.set("flow_commission", key + (y,), conversion_coefficient=float(c if is_number(c) else 1.0) * k[y])
                c = b.get("flow_milestone", key + (y,), "operational_cost")
                if is_number(c):
                    b.set("flow_milestone", key + (y,), operational_cost=float(c) * k[y])
                if charging and main:
                    for prm in ("investment_cost", "fixed_cost"):
                        c = b.get("asset_commission", (a, y), prm)
                        if is_number(c):
                            b.set("asset_commission", (a, y), **{prm: float(c) * k[y]})
            if len(set(k.values())) > 1:
                self.report.note(f"Flow {key[0]}->{key[1]}: storage efficiency varies by year; capacities use the first year.")
        for s, lim in storage_limits.items():
            total = max(lim.values())
            s_cap = b.get("asset", (s,), "capacity")
            n_s = b.get("asset_both", (s, y0, y0), "initial_units") or 0.0
            if not is_number(s_cap) or s_cap >= self.big:
                b.set("asset", (s,), capacity=total)
                for y in self.years:
                    b.set("asset_both", (s, y, y), initial_units=1.0)
                if lim["in"] and lim["out"] and lim["in"] != lim["out"]:
                    self.report.note(f"Storage {s}: Tulipa has one power capacity; the larger of the direct unit charging ({lim['in']}) and discharging ({lim['out']}) capacities is used.")
            elif total < float(s_cap) * float(n_s) - 1e-9:
                self.report.note(f"Storage {s}: unit flow capacities ({total}) are below the storage power capacity; Tulipa uses the storage power capacity.")

    # ------------------------------------------------------------ relationships and groups
    def build_relationships(self):
        params = {
            "equality": ("==", "equality_ratio", "equality_constant"),
            "greater_than": (">=", "greater_than_ratio", "greater_than_constant"),
            "less_than": ("<=", "less_than_ratio", "less_than_constant"),
        }
        for byname in self.ines.entities.get("unit_flow__unit_flow", []):
            classes = self.ines.element_classes.get(("unit_flow__unit_flow", byname), [])
            f1 = (classes[0], byname[0:2]) if classes else None
            f2 = (classes[1], byname[2:4]) if len(classes) > 1 else None
            k1, k2 = self.ines_flow_to_tulipa.get(f1), self.ines_flow_to_tulipa.get(f2)
            used = False
            for _, (sense, rp, cp) in params.items():
                ratio, const = self.g("unit_flow__unit_flow", byname, rp), self.g("unit_flow__unit_flow", byname, cp)
                if ratio is None and const is None:
                    continue
                if k1 is None or k2 is None:
                    self.report.skip(f"unit_flow__unit_flow.{rp if ratio is not None else cp}", [byname], "One of the flows is not a Tulipa flow.")
                    continue
                if used:
                    self.report.skip(f"unit_flow__unit_flow.{rp if ratio is not None else cp}", [byname], "Tulipa allows one relationship (one sense) per flow pair.")
                    continue
                used = True
                for y in self.years:
                    row = {"sense": sense}
                    r, c = self.val(ratio, y), self.val(const, y)
                    if is_number(r):
                        row["ratio"] = float(r)
                    if is_number(c):
                        row["constant"] = float(c)
                    self.b.set("flows_relationships", k1 + k2 + (y,), **row)

    def build_groups(self):
        plan = [
            ("invest_max_period", "use_only_investment_units", "<="),
            ("invest_min_period", "use_only_investment_units", ">="),
            ("max_cumulative", "use_available_units", "<="),
            ("min_cumulative", "use_available_units", ">="),
        ]
        members = {}
        for s, u in self.ines.entities.get("set__unit", []):
            if u in self.unit_asset:
                members.setdefault(s, []).append(self.unit_asset[u])
        for s in self.ines.names("set"):
            present = [p for p in plan if self.ines.has("set", s, p[0])]
            for prm, method, sense in present:
                value = self.g("set", s, prm)
                if not members.get(s):
                    self.report.skip(f"set.{prm}", [s], "Only sets with unit members are mapped to Tulipa investment groups.")
                    continue
                name = s if len(present) == 1 else f"{s}_{prm}"
                for y in self.years:
                    v = self.val(value, y)
                    if not is_number(v):
                        continue
                    self.b.set("investment_group_asset", (name, y), invest_method=method, constraint_sense=sense, rhs=float(v))
                    for a in members[s]:
                        cap = self.b.get("asset", (a,), "capacity", 0.0)
                        self.b.set("investment_group_asset_membership", (name, y, a), coefficient=float(cap))

    # ------------------------------------------------------------ run
    def run(self):
        self._pending_commodity_price = []
        self._pending_binary_limits = []
        self.build_time()
        self.classify()
        self.build_assets()
        self.build_units()
        self.build_nodes()
        self.build_links()
        self.apply_commodity_prices()
        self.apply_direct()
        self._storage_unit_flows()
        self._binary_storage_limits()
        self.build_relationships()
        self.build_groups()
        self.build_partitions()
        for (cls, param), ents in self.ines.unused().items():
            self.report.skip(f"{cls}.{param}", ents)
        return self.b.dataframes()


def _pick(value, i, n):
    if isinstance(value, Array):
        vals = list(value.values)
        return vals[i] if i < len(vals) else None
    if n > 1 and i > 0:
        return None
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description="Transform INES data into Tulipa input data.")
    parser.add_argument("input", help="INES sqlite file/url or INES JSON file")
    parser.add_argument("--duckdb", help="Output DuckDB file")
    parser.add_argument("--csv", help="Output folder for Tulipa CSV files")
    parser.add_argument("--scenario", help="INES scenario to use")
    parser.add_argument("--alternative", action="append", help="INES alternative(s) to use, later ones override earlier ones")
    parser.add_argument("--report", help="Conversion report (markdown). Default: next to the output")
    args = parser.parse_args(argv)
    if not args.duckdb and not args.csv:
        parser.error("Give --duckdb and/or --csv")

    mapping = load_mapping()
    report = Report(f"INES -> Tulipa conversion report for {os.path.basename(args.input)}", mapping["not_transformed_reasons"]["ines"])
    db = open_ines_source(args.input)
    try:
        ines = InesData(db, args.scenario, args.alternative)
    finally:
        db.close()
    schema = load_tulipa_schema()
    frames = InesToTulipa(ines, schema, mapping, load_settings(), report).run()
    write_tulipa(frames, schema, args.duckdb, args.csv)
    report_path = args.report or (os.path.splitext(args.duckdb)[0] if args.duckdb else args.csv.rstrip("/\\")) + "_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report.to_markdown())
    report.print_summary()
    print(f"Tulipa data written ({', '.join(x for x in (args.duckdb, args.csv) if x)}); report: {report_path}")


if __name__ == "__main__":
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    main()
