# INES <-> Tulipa transformation report

This report describes what `ines_to_tulipa.py` and `tulipa_to_ines.py` transform, what they do not transform, and how
the transformations were tested. Each run also writes its own report that lists the parameters of that dataset that
were not transformed.

Versions: INES spec as in `ines-spec-template.json`, TulipaEnergyModel 0.23.2 (`tulipa_input_schemas.json`).

## How the two models are matched

| INES | Tulipa |
|---|---|
| `period` (+ `solve_pattern.period`, `start_time`, `duration`, `time_resolution`, `system.timeline`) | milestone year (from the 4 digits in the period name, else the start year), one representative period covering the whole INES period, weight 1 (`rep_periods_data`, `timeframe_data`, `rep_periods_mapping`) |
| `system.discount_rate` | `model_parameters.discount_rate` (`discount_year` = first milestone year) |
| `node` with `node_type` balance (and other non-storage, non-commodity types) | `consumer` asset |
| `node.flow_profile` (negative = demand), `flow_annual`, `flow_scaling_method` | `peak_demand` + `demand` profile (positive inflow to a storage: `storage_inflows` + `inflows` profile) |
| `node.penalty_downward = 0` / `penalty_upward = 0` | `consumer_balance_sense` `>=` / `<=` |
| `node.penalty_upward > 0` / `penalty_downward > 0` | slack assets `<node>_penalty_upward` (producer) / `<node>_penalty_downward` (consumer) with the penalty as `operational_cost` |
| `node` storage | `storage` asset: `storage_capacity` -> `capacity_storage_energy`, `storages_existing` -> `initial_storage_units`, storage investment/fixed cost, lifetime, interest rate, integer, loss, `storage_state_fix` (fix_start) -> `initial_storage_level`, `storage_state_upper/lower_limit` -> `max/min_storage_level` profiles, `storage_investment_method` -> `storage_method_energy = optimize_storage_capacity` |
| `node` commodity consumed only by single-input units | no asset: `commodity_price` (+ input flow `other_operational_cost`) -> `flow_milestone.commodity_price` of the unit outputs, `unit.efficiency` -> `producer_efficiency` (time series price -> `commodity_price` profile) |
| other `node` commodity | producer asset with unlimited capacity (or a `>=` consumer sink if it only receives flows) |
| `unit` without (non-commodity) inputs | `producer` asset |
| `unit` with inputs | `conversion` asset, `unit.efficiency` -> `conversion_efficiency` |
| `unit.efficiency` as a map over operating points (e.g. `0.4: 0.5, 1.0: 0.55`) | lowest operating point -> `asset.min_operating_point`; the full-load (highest point) efficiency is used as the efficiency |
| `unit.efficiency` as a map over operating points (e.g. `0.4: 0.5, 1.0: 0.55`) | lowest operating point -> `asset.min_operating_point`; the full-load (highest point) efficiency is used as the efficiency |
| `unit__to_node` / `node__to_unit` | flows unit -> node / node -> unit |
| main output `capacity` (else input capacity x efficiency) | `asset.capacity`; other outputs get `capacity_coefficient = main / own capacity` |
| `units_existing` | `asset_both.initial_units` |
| `investment_method`, `units_invest_max/min_period`, `units_max/min_cumulative`, `retirement_method` | `investable`, `investment_max/min_limit` (units x capacity), `max/min_available_units`, `decommissionable` |
| `lifetime`, `interest_rate`, `investment_uses_integer`, `investment_cost`, `fixed_cost`, `other_operational_cost`, `conversion_coefficient` | `technical_lifetime` + `economic_lifetime`, `discount_rate`, `investment_integer`, `investment_cost`, `fixed_cost`, `operational_cost`, `conversion_coefficient` |
| `startup_method`, `min_uptime`, `min_downtime` (minutes), `online_cost` | `unit_commitment` (basic, 3var when min up/down times are given), `unit_commitment_integer`, `minimum_up/down_time` (hours), `units_on_cost` |
| `ramp_method`, `ramp_limit_up/down` (p.u./minute) | `ramping`, `max_ramp_up/down` (p.u./hour) |
| `unit.availability` x output `profile_limit_upper` (`profile_method` upper_limit) | `availability` profile |
| `link` between two non-storage nodes | transport flow: `capacity`, `links_existing` -> export and import units, investment/retirement, cost, lifetime, `operational_cost`, `availability` profile |
| `link` between a node and a storage | two flows, storage power `capacity` = link capacity, `links_existing` -> `initial_units`, link investment -> storage power investment, link efficiencies -> charging/discharging efficiency |
| `link.transfer_method = exact_integer` on a link between a storage and a node | `asset.use_binary_storage_method = binary` (no simultaneous charging and discharging); Tulipa `relaxed_binary` is also written back as `exact_integer` |
| `constraint` (sense `equal`, constant 0) with `node.constraint_storage_count_coefficient` (-1) on a storage and `link.constraint_link_count_coefficient` (r) on its link | `storage_method_energy = use_fixed_energy_to_power_ratio`, `energy_to_power_ratio = -(link coefficient / storage coefficient) x storage_capacity / link capacity` (the INES constraint counts units, Tulipa's ratio is MWh per MW) |
| `link` with `transfer_method = one_way` | one non-transport flow |
| `unit_flow__unit_flow` equality/greater_than/less_than ratio and constant | `flows_relationships` (`==`, `>=`, `<=`) |
| `set` + `set__unit` with `invest_max/min_period`, `max/min_cumulative` | `investment_group_asset` (+ membership with coefficient = asset capacity) |
| INES alternatives/scenarios | chosen with `--scenario` / `--alternative`; Tulipa has no scenarios |

Tulipa -> INES uses the same correspondences in reverse. Extra rules in that direction:

- A Tulipa producer with `commodity_price` becomes an INES unit fed by a new commodity node `<asset>_commodity`
  (`producer_efficiency` -> `unit.efficiency`).
- A flow between two units (e.g. producer -> conversion) goes through a new INES balance node `<from>__<to>`.
- Flows between two node-like assets (consumer, storage) become INES links: transport flows are bidirectional links,
  storage in/out flow pairs are `regular_linear` links carrying the storage power capacity, other flows are `one_way`.
- Representative periods are written one after another on the INES timeline starting at 1 January of the milestone
  year; values that vary per milestone year become maps over the INES periods.
- `asset.min_operating_point` becomes an INES efficiency map `{min_operating_point: eff, 1.0: eff}` (the efficiency
  repeated, since Tulipa has no part-load efficiency); producers without an efficiency use 1.0.
- `asset.min_operating_point` becomes an INES efficiency map `{min_operating_point: eff, 1.0: eff}` (the efficiency
  repeated, since Tulipa has no part-load efficiency); producers without an efficiency use 1.0.
- A Tulipa producer with a capacity >= `unlimited_capacity` (settings) is taken as an INES commodity node, and the
  `_penalty_upward/_penalty_downward` slack assets become node penalties again.

## Parameters that are not transformed

### INES -> Tulipa

| INES parameter(s) | Reason |
|---|---|
| `unit.startup_cost`, `shutdown_cost`, `startup_cost_tiers`, `cooling_time_to_tiers` | Tulipa has no start-up/shut-down costs. Tiered/trajectory start-ups are approximated by plain unit commitment. |
| all emission parameters (`node.co2_content`, `*_co2_emission*`, `*_nox_*`, `*_so2_*`, `set.co2_*`) | Tulipa has no emission accounting (model emissions as an explicit flow into a consumer instead). |
| `reserve`, `node__reserve`, `unit__node__reserve`, `link__node__reserve`, `set__reserve` parameters | Tulipa has no reserves. |
| `constraint.*`, `*constraint_*_coefficient` | Tulipa has no user constraints. |
| `link.efficiency`, `node__link__node.efficiency` on transport links | Tulipa transport flows are lossless (efficiencies are used only for links to storages). |
| `node__link__node.capacity` that differs from `link.capacity` | Tulipa transport flows have one capacity for both directions. |
| `link.*` investment parameters of a storage that has several links | Tulipa has one power capacity per storage. |
| `link.links_max/min_cumulative`, `node.storages_max/min_cumulative` | No available-unit limits for flows / storage energy units in Tulipa. |
| `*_fix_cumulative`, `*_invest_fix_period`, `*_retire_period` | Tulipa cannot fix investments, unit counts or scheduled retirements. |
| `set.invest_max/min_total` | Tulipa group limits are per milestone year. |
| `salvage_value`, `storage_salvage_value` | Tulipa computes salvage values internally. |
| `profile_fix`, `profile_limit_lower`, `ramp_cost`, `flow_max/min_cumulative`, `inertia_constant`, `is_non_synchronous` | No Tulipa counterpart. |
| `node.commodity_limit`, `commodity_price_ladder`, `spill_*`, `storage_cost_of_stored_energy`, `storage_loss_absolute`, `storage_loss_from_storage_capacity`, `storage_state_value*`, `storage_state_binding_method`, `storage_state_*_values` | No Tulipa counterpart. |
| `node.flow_profile` > 0 on a balance node, < 0 on a storage | Tulipa consumers only have non-negative demand and storages only non-negative inflows. |
| part-load efficiency (different efficiencies in an operating-point `efficiency` map) | Tulipa has one efficiency; the full-load value is used (the minimum operating point is kept). An operating-point map that varies per period uses the first period's minimum operating point. |
| part-load efficiency (different efficiencies in an operating-point `efficiency` map) | Tulipa has one efficiency; the full-load value is used (the minimum operating point is kept). An operating-point map that varies per period uses the first period's minimum operating point. |
| `node__node.*` (diffusion) | No Tulipa counterpart. |
| `set.time_resolution`, `solve_pattern.time_resolution_scope` | Set based resolutions are not mapped to Tulipa partitions. |
| `solve_pattern.rolling_*`, `solve_mode`, `includes_solve_pattern`, stochastic parameters | Rolling horizon is a Tulipa run option; INES stochastics are not mapped. |
| `period.years_represented` | Tulipa derives it from the milestone years. |
| `unit` without outputs | A Tulipa conversion asset needs an output. |

Approximations (listed under "Notes" in each run report): time series used where Tulipa needs a constant are averaged
(e.g. time-varying operational costs, efficiencies); secondary output capacities are combined into one weighted
capacity constraint; a capacity given only on the input is converted to an output capacity with the efficiency;
commodity sources and slack assets get the `unlimited_capacity`; storages without a link get unlimited power capacity.

### Tulipa -> INES

| Tulipa column/table | Reason |
|---|---|
| `rep_periods_mapping` weights, `timeframe_data` (when not 1 period -> 1 rep period, weight 1) | INES has no representative periods; rep periods are written one after another and their weights are lost. |
| `stochastic_scenario`, `rep_periods_mapping.scenario`, `model_parameters.risk_aversion_*` | INES stochastic structure (forecasts) is different; no CVaR. |
| `asset.economic_lifetime` / `flow.economic_lifetime` when different from technical lifetime | INES has one `lifetime` (technical lifetime is used). |
| `asset.vintage_method`, rows of commission years that are not milestone years, vintage profiles | INES has no vintages before/between its periods; existing units of several commission years are summed. |
| `asset.use_inter_period_constraints`, `inter_period_storage_level_bounds`, `assets_timeframe_*`, `profiles_timeframe`, `asset_milestone.min/max_energy_timeframe_partition` | No inter-period structure in INES. |
| `assets_rep_periods_partitions`, `flows_rep_periods_partitions` | No flexible per-asset/per-flow resolution in INES. |
| `asset.use_binary_storage_method` or `energy_to_power_ratio` on a storage without a two-way link to a node | No INES counterpart (binary storage needs an `exact_integer` link, the energy-to-power ratio a unit-count constraint between storage and link). |
| storage `capacity` (power) when the storage has no link to a consumer/storage | INES storage nodes have no power capacity (it is in the connected units). |
| `flow.carrier` | INES has no carrier attribute. |
| `flow_commission.storage_coefficient`; `capacity_coefficient`/`conversion_coefficient` on node-to-node flows | INES links have no coefficients. |
| `flow_both.initial_import_units` different from export units | INES links have one unit count for both directions. |
| transport attributes of a flow from a unit (`is_transport` on producer/conversion flows) | INES unit flows are not links. |
| `flow_milestone.dc_opf`, `reactance`, `model_parameters.power_system_base` | INES has no reactance. |
| `flows_relationships` involving a node-to-node flow | INES flow relationships are between unit flows only. |
| investment groups whose coefficients are not the asset capacities, or with non-unit members | INES set limits are in MW of unit capacity. |
| `model_parameters.discount_year` different from the first milestone year | INES discounts to the first period. |
| `profiles-wide` and other non-schema tables | Need TulipaClustering first. |

## Testing

`python tests/run_roundtrip.py --solve` (outputs in `test_output/`):

1. Tulipa -> INES (sqlite and JSON) -> Tulipa for Tiny, Storage, Multi-year Investments, UC-ramping, Norse,
   Variable Resolution and tutorials 1, 2, 3 and 6-simple-method.
2. INES -> Tulipa (DuckDB and CSV) -> INES for the INES certification cases storage, unit_commitment,
   investment__fixed, network__mesh and combined_system.
3. A made-up INES model (`tests/data/ines_test_model.json`): two periods, demand/inflow time series, scale_to_annual,
   penalties, wind with profile and investment limits, gas CCGT with unit commitment and ramping, CHP with two outputs
   and a flow ratio, heat pump, electrolyser with input capacity, fuel cell with a non-absorbed commodity, hydro reservoir,
   battery with an `exact_integer` link, transmission link, investment group and a reserve. INES -> Tulipa -> INES -> Tulipa.

All converted datasets were solved with TulipaEnergyModel 0.23.2 + HiGHS. Because INES cannot hold representative
period weights or two lifetimes, each Tulipa case is also solved as a "reference" copy where every representative
period has weight 1 and economic lifetime = technical lifetime. Objective values:

| Case | Original | Reference | Tulipa -> INES -> Tulipa | Cause of remaining difference |
|---|---|---|---|---|
| Tiny | 278067.2 | 7403.26 | 7403.26 | - |
| UC-ramping | 302419.5 | 3294.23 | 3294.23 | - |
| tutorial-1 | 2.17577e8 | 2.17577e8 | 2.17577e8 | - |
| tutorial-2 / tutorial-3 | 1.48541e8 | 1.48541e8 | 1.48541e8 | - |
| tutorial-6-simple-method | 8.51171e6 | 1.39591e6 | 1.39591e6 | - |
| Variable Resolution | 28.4587 | 28.4587 | 28.4365 | per-asset/flow partitions |
| Storage | 4498.4 | 1989.25 | 2023.39 | rep periods concatenated into one cycle, inter-period storage |
| Norse | 2.52499e8 | 2.43419e7 | 2.45682e7 | partitions, binary storage on a one-way link, inter-period constraints |
| Multi-year Investments | 4.63370e6 | 443311.7 | 645652.0 | vintages (compact methods, commission years 2020/2025) |
| INES test model | - | - | 3.51260e7 (INES -> Tulipa) = 3.51260e7 (after INES -> Tulipa -> INES -> Tulipa) | - |

The INES certification cases all solve (storage 12.5 matches a hand calculation: 20 MWh missing from the renewable is
delivered through the storage at 2.5 /MWh).
