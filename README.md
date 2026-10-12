# ines-tulipa

Transformations between the [INES](https://github.com/ines-tools/ines-spec) energy system data format and the input data of
[TulipaEnergyModel.jl](https://github.com/TulipaEnergy/TulipaEnergyModel.jl) (v0.23).

| Script | From | To |
|---|---|---|
| `ines_to_tulipa.py` | INES Spine DB (sqlite file or url) or INES JSON export | Tulipa DuckDB file and/or folder of Tulipa CSV files |
| `tulipa_to_ines.py` | Tulipa DuckDB file or folder of Tulipa CSV files | INES Spine DB (sqlite) or INES JSON |

Both scripts use the same mapping (`ines_tulipa_mapping.yaml` plus the shared code in `ines_tulipa_common.py`) in
opposite directions. The two models are not identical, so a round trip does not always give back the original data;
see `REPORT.md` for what is and is not transformed.

## Install

```
python -m venv .venv
.venv\Scripts\python -m pip install spinedb_api duckdb pandas pyyaml
```

## Use

```
python ines_to_tulipa.py model.sqlite --duckdb tulipa.duckdb --csv tulipa_csv
python ines_to_tulipa.py model.json --csv tulipa_csv --scenario base
python tulipa_to_ines.py tulipa_csv ines.sqlite
python tulipa_to_ines.py tulipa.duckdb ines.json
```

- `--scenario` picks the INES scenario (default: the only scenario, otherwise all alternatives in database order).
  `--alternative A --alternative B` picks alternatives directly (later ones override earlier ones).
- Every run writes a markdown report next to the output (`--report` to choose the file) listing the input parameters
  that were not transformed (with the reason) and the approximations made.
- CSV files use the Tulipa file names (`asset.csv`, `flow-milestone.csv`, ...) and can be read with
  `TulipaIO.read_csv_folder`. The DuckDB file holds the same tables.
- `settings.yaml` holds a few names and the capacity used for assets that INES does not limit.
- `tulipa_input_schemas.json` (Tulipa input schema) and `ines-spec-template.json` (INES specification) are copies of the
  versions the mapping was written for.

## Tests

`tests/run_roundtrip.py` converts the Tulipa test and tutorial cases to INES and back, the INES certification cases (from
the ines-spec repository) to Tulipa and back, and a made-up INES model (`tests/data/ines_test_model.json`, created by
`tests/make_ines_test_model.py`). With `--solve` it solves the original, a reference copy without the features INES
cannot hold, and the round-tripped Tulipa data with TulipaEnergyModel. This needs a Julia environment in `.julia_env`
with TulipaEnergyModel, TulipaIO, DuckDB, DBInterface and HiGHS (see `tests/run_tulipa.jl`).

```
python tests/run_roundtrip.py --solve
```
