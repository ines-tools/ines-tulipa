"""Shared I/O and helpers for the INES <-> Tulipa transformations."""

import json
import math
import os
import re
from collections import defaultdict

import duckdb
import numpy as np
import pandas as pd
import yaml
from spinedb_api import (
    Array,
    DatabaseMapping,
    DateTime,
    Duration,
    Map,
    TimeSeries,
    export_data,
    import_data,
)
from spinedb_api.parameter_value import from_database, from_dict

HERE = os.path.dirname(os.path.abspath(__file__))
TULIPA_SCHEMA_FILE = os.path.join(HERE, "tulipa_input_schemas.json")
INES_TEMPLATE_FILE = os.path.join(HERE, "ines-spec-template.json")


def load_yaml(name):
    with open(os.path.join(HERE, name), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_mapping():
    return load_yaml("ines_tulipa_mapping.yaml")


def load_settings():
    return load_yaml("settings.yaml") or {}


# ---------------------------------------------------------------- Tulipa side

def load_tulipa_schema():
    with open(TULIPA_SCHEMA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


TULIPA_KEYS = {
    "asset": ["asset"],
    "asset_both": ["asset", "milestone_year", "commission_year"],
    "asset_commission": ["asset", "commission_year"],
    "asset_milestone": ["asset", "milestone_year"],
    "assets_profiles": ["asset", "commission_year", "profile_type"],
    "assets_rep_periods_partitions": ["asset", "milestone_year", "rep_period"],
    "assets_timeframe_partitions": ["asset", "milestone_year"],
    "assets_timeframe_profiles": ["asset", "milestone_year", "scenario", "profile_type"],
    "flow": ["from_asset", "to_asset"],
    "flow_both": ["from_asset", "to_asset", "milestone_year", "commission_year"],
    "flow_commission": ["from_asset", "to_asset", "commission_year"],
    "flow_milestone": ["from_asset", "to_asset", "milestone_year"],
    "flows_profiles": ["from_asset", "to_asset", "milestone_year", "profile_type"],
    "flows_relationships": ["flow_1_from_asset", "flow_1_to_asset", "flow_2_from_asset", "flow_2_to_asset", "milestone_year"],
    "flows_rep_periods_partitions": ["from_asset", "to_asset", "milestone_year", "rep_period"],
    "investment_group_asset": ["name", "milestone_year"],
    "investment_group_asset_membership": ["group_name", "milestone_year", "asset"],
    "model_parameters": [],
    "profiles_rep_periods": ["profile_name", "milestone_year", "rep_period", "timestep"],
    "profiles_timeframe": ["profile_name", "milestone_year", "period"],
    "rep_periods_data": ["milestone_year", "rep_period"],
    "rep_periods_mapping": ["milestone_year", "period", "rep_period", "scenario"],
    "stochastic_scenario": ["scenario"],
    "timeframe_data": ["milestone_year", "period"],
}

DUCKDB_TYPES = {"VARCHAR": "VARCHAR", "DOUBLE": "DOUBLE", "INTEGER": "INTEGER", "BOOLEAN": "BOOLEAN"}


class TulipaBuilder:
    """Collects Tulipa rows keyed by table and primary key."""

    def __init__(self, schema):
        self.schema = schema
        self.rows = {table: {} for table in schema}

    def set(self, table, key, **values):
        cols = TULIPA_KEYS[table]
        if isinstance(key, dict):
            key = tuple(key[c] for c in cols)
        key = tuple(key)
        row = self.rows[table].setdefault(key, dict(zip(cols, key)))
        for col, val in values.items():
            if col not in self.schema[table]:
                raise KeyError(f"Column {col} not in Tulipa table {table}")
            row[col] = val
        return row

    def get(self, table, key, col, default=None):
        row = self.rows[table].get(tuple(key))
        if row is None:
            return default
        val = row.get(col)
        return default if val is None else val

    def has(self, table, key):
        return tuple(key) in self.rows[table]

    def dataframes(self):
        out = {}
        for table, cols in self.schema.items():
            columns = sorted(cols, key=lambda c: (c not in TULIPA_KEYS[table], TULIPA_KEYS[table].index(c) if c in TULIPA_KEYS[table] else 0, c))
            df = pd.DataFrame(list(self.rows[table].values()), columns=columns)
            for col in columns:
                typ = cols[col]["type"]
                default = cols[col].get("default")
                if default is not None:
                    df[col] = [default if _isnull(v) else v for v in df[col]]
                if typ == "INTEGER":
                    df[col] = pd.array([None if _isnull(v) else int(round(v)) for v in df[col]], dtype="Int64")
                elif typ == "DOUBLE":
                    df[col] = pd.array([None if _isnull(v) else float(v) for v in df[col]], dtype="Float64")
                elif typ == "BOOLEAN":
                    df[col] = pd.array([None if _isnull(v) else bool(v) for v in df[col]], dtype="boolean")
                else:
                    df[col] = pd.array([None if _isnull(v) else str(v) for v in df[col]], dtype="string")
            out[table] = df
        return out


def _isnull(v):
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def write_tulipa(frames, schema, duckdb_path=None, csv_dir=None, skip_empty_csv=False):
    """Write Tulipa tables to a DuckDB file and/or a folder of CSV files."""
    if duckdb_path and os.path.exists(duckdb_path):
        os.remove(duckdb_path)
    con = duckdb.connect(duckdb_path or ":memory:")
    try:
        for table, df in frames.items():
            cols = schema[table]
            col_defs = ", ".join(f'"{c}" {DUCKDB_TYPES[cols[c]["type"]]}' for c in df.columns)
            con.execute(f'CREATE TABLE "{table}" ({col_defs})')
            if len(df):
                con.register("df_view", df)
                con.execute(f'INSERT INTO "{table}" SELECT * FROM df_view')
                con.unregister("df_view")
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)
            for table, df in frames.items():
                path = os.path.join(csv_dir, table.replace("_", "-") + ".csv")
                if skip_empty_csv and len(df) == 0:
                    if os.path.exists(path):
                        os.remove(path)
                    continue
                if os.path.exists(path):
                    os.remove(path)
                con.execute(f"COPY \"{table}\" TO '{path.replace(chr(92), '/')}' (HEADER, DELIMITER ',')")
    finally:
        con.close()


def read_tulipa(source, schema):
    """Read Tulipa input tables from a DuckDB file or a folder of CSV files.

    Missing tables are returned empty and missing columns are added as nulls.
    """
    frames = {}
    if os.path.isdir(source):
        con = duckdb.connect(":memory:")
        for fname in sorted(os.listdir(source)):
            if not fname.endswith(".csv"):
                continue
            table = fname[:-4].replace("-", "_")
            path = os.path.join(source, fname).replace("\\", "/")
            frames[table] = con.execute(f"SELECT * FROM read_csv_auto('{path}', header=true)").df()
        con.close()
    else:
        con = duckdb.connect(source, read_only=True)
        for (table,) in con.execute("SELECT table_name FROM information_schema.tables").fetchall():
            frames[table] = con.execute(f'SELECT * FROM "{table}"').df()
        con.close()
    out = {}
    for table, cols in schema.items():
        df = frames.get(table, pd.DataFrame())
        for col in cols:
            if col not in df.columns:
                df[col] = None
        out[table] = df.astype(object).where(pd.notna(df), None)
    extra = {t: f for t, f in frames.items() if t not in schema}
    return out, extra


# ---------------------------------------------------------------- INES side

def _parse_json_value(v):
    if isinstance(v, dict):
        return from_dict(v)
    return v


def _prepare_import(data):
    data = dict(data)
    data.pop("parameter_types", None)
    if "parameter_values" in data:
        data["parameter_values"] = [
            [*row[:3], _parse_json_value(row[3]), *row[4:]] for row in data["parameter_values"]
        ]
    if "parameter_definitions" in data:
        data["parameter_definitions"] = [
            [row[0], row[1], _parse_json_value(row[2]) if len(row) > 2 else None, *row[3:]]
            for row in data["parameter_definitions"]
        ]
    return data


def ines_url(path):
    if "://" in path:
        return path
    return "sqlite:///" + os.path.abspath(path)


def open_ines_source(path):
    """Return a DatabaseMapping for an INES sqlite url/file or an INES JSON file."""
    if path.lower().endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        db = DatabaseMapping("sqlite://", create=True)
        _, errors = import_data(db, **_prepare_import(data))
        if errors:
            raise RuntimeError(f"Errors importing {path}: {errors[:5]}")
        db.commit_session("Imported INES json")
        return db
    return DatabaseMapping(ines_url(path))


class InesData:
    """INES entities and the parameter values of the selected alternatives.

    Every read through get() is recorded so that unused parameters can be reported.
    """

    def __init__(self, db, scenario=None, alternatives=None):
        self.alternative_order = self._alternative_order(db, scenario, alternatives)
        rank = {alt: i for i, alt in enumerate(self.alternative_order)}
        inactive = {}
        for ea in db.get_entity_alternative_items():
            if ea["alternative_name"] in rank:
                key = (ea["entity_class_name"], tuple(ea["entity_byname"]))
                prev = inactive.get(key)
                if prev is None or rank[ea["alternative_name"]] >= prev[0]:
                    inactive[key] = (rank[ea["alternative_name"]], not ea["active"])
        self.entities = defaultdict(list)
        self.element_classes = {}
        for item in db.get_entity_items():
            key = (item["entity_class_name"], tuple(item["entity_byname"]))
            if inactive.get(key, (0, False))[1]:
                continue
            self.entities[item["entity_class_name"]].append(tuple(item["entity_byname"]))
            if item["element_id_list"]:
                self.element_classes[key] = [
                    db.get_entity_item(id=eid)["entity_class_name"] for eid in item["element_id_list"]
                ]
        self.values = {}
        chosen = {}
        for pv in db.get_parameter_value_items():
            alt = pv["alternative_name"]
            if alt not in rank:
                continue
            key = (pv["entity_class_name"], tuple(pv["entity_byname"]), pv["parameter_definition_name"])
            if key in chosen and chosen[key] > rank[alt]:
                continue
            chosen[key] = rank[alt]
            self.values[key] = pv["parsed_value"]
        self.used = set()

    @staticmethod
    def _alternative_order(db, scenario, alternatives):
        if alternatives:
            return list(alternatives)
        scenarios = {s["name"] for s in db.get_scenario_items()}
        if scenario is None and len(scenarios) == 1:
            scenario = next(iter(scenarios))
        if scenario is not None:
            if scenario not in scenarios:
                raise ValueError(f"Scenario {scenario} not found. Available: {sorted(scenarios)}")
            sas = [sa for sa in db.get_scenario_alternative_items() if sa["scenario_name"] == scenario]
            return [sa["alternative_name"] for sa in sorted(sas, key=lambda sa: sa["rank"])]
        return [a["name"] for a in db.get_alternative_items()]

    def get(self, cls, byname, param, default=None):
        if isinstance(byname, str):
            byname = (byname,)
        key = (cls, tuple(byname), param)
        if key in self.values:
            self.used.add(key)
            return self.values[key]
        return default

    def has(self, cls, byname, param):
        if isinstance(byname, str):
            byname = (byname,)
        return (cls, tuple(byname), param) in self.values

    def mark(self, cls, byname, param):
        if isinstance(byname, str):
            byname = (byname,)
        key = (cls, tuple(byname), param)
        if key in self.values:
            self.used.add(key)

    def names(self, cls):
        return [b[0] if len(b) == 1 else b for b in self.entities.get(cls, [])]

    def unused(self):
        out = defaultdict(list)
        for key in self.values:
            if key not in self.used:
                out[(key[0], key[2])].append(key[1])
        return out


def write_ines(target, entities, values, alternative="Base", scenario="base"):
    """Write INES data to a sqlite url/file or to a JSON file.

    entities: list of (class, byname tuple); values: list of (class, byname, param, value).
    """
    with open(INES_TEMPLATE_FILE, "r", encoding="utf-8") as f:
        template = json.load(f)
    is_json = target.lower().endswith(".json")
    url = "sqlite://" if is_json else ines_url(target)
    with DatabaseMapping(url, create=True) as db:
        if not db.get_entity_class_items():
            _, errors = import_data(db, **_prepare_import(template))
            if errors:
                raise RuntimeError(f"Errors importing INES template: {errors[:5]}")
        db.purge_items("parameter_value")
        db.purge_items("entity")
        db.purge_items("alternative")
        db.purge_items("scenario")
        data = {
            "alternatives": [[alternative, ""]],
            "scenarios": [[scenario, False, ""]],
            "scenario_alternatives": [[scenario, alternative, None]],
            "entities": [[cls, byname if len(byname) > 1 else byname[0], None] for cls, byname in entities],
            "parameter_values": [[cls, byname if len(byname) > 1 else byname[0], p, v, alternative] for cls, byname, p, v in values],
        }
        _, errors = import_data(db, **data)
        if errors:
            raise RuntimeError(f"Errors writing INES data: {errors[:10]}")
        db.commit_session("Imported from Tulipa")
        if is_json:
            exported = export_data(db, parse_value=from_database)
            exported.pop("parameter_types", None)
            for key in ("parameter_values", "parameter_definitions"):
                rows = []
                for row in exported.get(key, []):
                    row = list(row)
                    idx = 3 if key == "parameter_values" else 2
                    if idx < len(row):
                        row[idx] = jsonable(row[idx])
                    rows.append(row)
                exported[key] = rows
            with open(target, "w", encoding="utf-8") as f:
                json.dump(exported, f, indent=2, default=str)


def jsonable(v):
    if hasattr(v, "to_dict"):
        d = v.to_dict()
        d["type"] = v.type_()
        return d
    if isinstance(v, (np.floating, np.integer)):
        return v.item()
    return v


# ------------------------------------------------------------ value helpers

def duration_hours(value):
    """Hours of a spinedb Duration / relativedelta / string like '1h', '15m', '1D'."""
    if value is None:
        return None
    if isinstance(value, Duration):
        value = value.value
    if isinstance(value, str):
        m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhDMY])\s*", value)
        if not m:
            raise ValueError(f"Cannot parse duration {value}")
        n, u = float(m.group(1)), m.group(2)
        return n * {"s": 1 / 3600, "m": 1 / 60, "h": 1, "D": 24, "M": 730, "Y": 8760}[u]
    rd = value
    return (
        (getattr(rd, "years", 0) or 0) * 8760
        + (getattr(rd, "months", 0) or 0) * 730
        + (getattr(rd, "days", 0) or 0) * 24
        + (getattr(rd, "hours", 0) or 0)
        + (getattr(rd, "minutes", 0) or 0) / 60
        + (getattr(rd, "seconds", 0) or 0) / 3600
    )


def to_timestamp(value):
    if value is None:
        return None
    if isinstance(value, DateTime):
        value = value.value
    return pd.Timestamp(value)


def ts_to_series(ts):
    idx = pd.DatetimeIndex(pd.to_datetime(np.asarray(ts.indexes)))
    return pd.Series(np.asarray(ts.values, dtype=float), index=idx).sort_index()


def is_map_of(value, keys):
    return isinstance(value, Map) and any(str(i) in keys for i in value.indexes)


def map_to_dict(value):
    return {str(i): v for i, v in zip(value.indexes, value.values)}


def value_list(value):
    """Turn an INES Array / Map / str into a list of strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Array):
        return [str(v) for v in value.values]
    if isinstance(value, Map):
        out = []
        for v in value.values:
            for x in value_list(v):
                if x not in out:
                    out.append(x)
        return out
    return [str(value)]


def per_year_value(year_values, periods_by_year):
    """Collapse {year: value} into an INES scalar or Map over period names."""
    vals = {y: v for y, v in year_values.items() if not _isnull(v)}
    if not vals:
        return None
    unique = {json.dumps(v, default=str) for v in vals.values()}
    if len(unique) == 1:
        return next(iter(vals.values()))
    years = sorted(vals)
    return Map([periods_by_year[y] for y in years], [vals[y] for y in years], index_name="period")


def is_number(v):
    return isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool)


def ceil_int(v):
    return int(math.ceil(v - 1e-9))


# ---------------------------------------------------------------- reporting

class Report:
    def __init__(self, title, reasons):
        self.title = title
        self.reasons = reasons or {}
        self.not_transformed = {}
        self.notes = []
        self.transformed = set()

    def note(self, text):
        if text not in self.notes:
            self.notes.append(text)

    def skip(self, name, entities, reason=None):
        entry = self.not_transformed.setdefault(name, {"entities": [], "reason": reason})
        for e in entities:
            if e not in entry["entities"]:
                entry["entities"].append(e)
        if reason and not entry["reason"]:
            entry["reason"] = reason

    def reason_for(self, name):
        if name in self.reasons:
            return self.reasons[name]
        cls = name.split(".")[0]
        if cls in self.reasons:
            return self.reasons[cls]
        param = name.split(".", 1)[-1]
        for key, text in self.reasons.items():
            if key.startswith("unit_flow.") and key.split(".", 1)[1] == param and cls in ("unit__to_node", "node__to_unit"):
                return text
        return ""

    def to_markdown(self):
        lines = [f"# {self.title}", ""]
        lines.append("## Not transformed")
        lines.append("")
        if not self.not_transformed:
            lines.append("Everything in the input was transformed.")
        else:
            lines.append("| Parameter | Count | Example entities | Reason |")
            lines.append("|---|---|---|---|")
            for name in sorted(self.not_transformed):
                entry = self.not_transformed[name]
                ents = entry["entities"]
                example = ", ".join(_fmt_entity(e) for e in ents[:3]) + (" ..." if len(ents) > 3 else "")
                reason = entry["reason"] or self.reason_for(name)
                lines.append(f"| {name} | {len(ents)} | {example} | {reason} |")
        lines.append("")
        lines.append("## Notes on approximations")
        lines.append("")
        lines.extend([f"- {n}" for n in self.notes] or ["None."])
        lines.append("")
        return "\n".join(lines)

    def print_summary(self):
        print(f"{len(self.not_transformed)} parameter(s) not transformed:")
        for name in sorted(self.not_transformed):
            print(f"  {name} ({len(self.not_transformed[name]['entities'])})")
        if self.notes:
            print(f"{len(self.notes)} note(s), see the report file.")


def _fmt_entity(e):
    if isinstance(e, tuple):
        return "/".join(str(x) for x in e)
    return str(e)
