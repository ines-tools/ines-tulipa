# INES <-> Tulipa transformation report

This report describes what `ines_to_tulipa.py` and `tulipa_to_ines.py` transform, what they do not transform, and how
the transformations were tested. Each run also writes its own report that lists the parameters of that dataset that
were not transformed.

Versions: INES spec as in `ines-spec-template.json`, TulipaEnergyModel 0.23.2 (`tulipa_input_schemas.json`).

## How the two models are matched

| INES | Tulipa |
|---|---|
| `period` (+ `solve_pattern.period`, `start_time`, `duration`, `time_resolution`, `system.timeline`) | milestone year (from the 4 digits in the period name, else the start year), one representative period covering the whole INES period, weight 1 (`rep_periods_data`, `timeframe_data`, `rep_periods_mapping`) |
| `solve_pattern.representative_period_mapping` (period -> representative period -> weight), `stochastic_representative_period_mapping` (period -> stochastic branch -> representative period -> weight), `period.duration` as an array of durations | one Tulipa representative period per mapped representative period (in mapping order, consecutive in the period's timeline, lengths from the `period.duration` array, else equal); each gets one timeframe period mapped with its weight (branch = `scenario`). Back: weights summed over the Tulipa timeframe periods per (milestone year, scenario, representative period), named `<period>_rp<k>`, written only when not all 1; `period.duration` becomes an array of the representative period durations |
| `unit__to_node.profile_limit_upper_representative_period` (representative period -> timestep -> value, with `profile_method` upper_limit) | availability profile of the unit asset per representative period (multiplied with `unit.availability` and a time series `profile_limit_upper`). Back: used for the availability of single-output units when the model has representative periods |
| `system.discount_rate`, `system.discount_year` | `model_parameters.discount_rate`, `discount_year` (default and back: the first milestone year, written to INES only when different) |
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
| main output `capacity` (else input capacity x efficiency) | `asset.capacity`; without `combined_capacity` other outputs get `capacity_coefficient = main / own capacity` (a stricter approximation of independent flow limits) |
| `unit.combined_capacity` + output `capacity_coefficient` c (default 1): sum(c x output) <= combined_capacity | each output gets `capacity_coefficient = asset capacity / combined_capacity x c` (asset capacity = combined_capacity if the main flow has none); flow capacities below their share are reported (Tulipa has one capacity per asset). Back: `combined_capacity` = asset capacity and `capacity_coefficient` = Tulipa coefficient (exact) for units with two or more limited outputs; investment/fixed costs of the main flow are scaled by its coefficient because the INES main flow capacity is capacity / coefficient. Direct storage flows use c / k (measured at the storage) |
| `unit__to_node.carrier`, `node__to_unit.carrier`, `node__link__node.carrier` | `flow.carrier` (a link's carrier goes to both of its Tulipa flows; a flow between two units puts it on both INES flows through the extra node) |
| `units_existing` | `asset_both.initial_units` |
| `unit.commission_date`, `link.commission_date`, `node.storage_commission_date` (existing assets) | existing units are 0 in milestone years before the year of the date (Tulipa's aggregated vintage method needs `commission_year = milestone_year`, so a date before the first period has no effect). Back: when Tulipa rows have a single commission year different from the milestone year, it becomes the date (1 January). |
| balance node `<unit>__<asset>` with a unit flow and a transport `link` | Back: a transport flow (`is_transport`) from or to a unit becomes a unit flow to a balance node `<unit>__<asset>` and a transport link with the flow's own capacity, existing units, investment, lifetime and operational cost (the unit flow keeps the unit capacity, coefficients and commodity price). For compact vintage assets the connection is shared by all vintage units. |
| unit `<storage>__<node>` without input (side output of a storage) | Back: a storage outflow with `storage_coefficient` 0 (e.g. the CO2 of a CAES) does not use stored energy, so it becomes an INES unit without input, with unlimited capacity and the flow's operational cost; flow relationships to it become `unit_flow__unit_flow`. |
| units `<name>_<year>` (one INES unit per vintage) | Back: a producer/conversion asset with a compact vintage method (`compact_profiles`, `compact_efficiencies`) is split into one unit per commission year that has existing units or is investable, named `<asset>_<commission year>`. Common parameters (type, capacity, lifetime, unit commitment, ramping, milestone-year costs and prices) are copied; vintage parameters come from the commission year (`asset_commission`, `flow_commission`: costs, efficiencies, capacity/conversion coefficients, `producer_efficiency`; vintage profiles). Existing units per period come from `asset_both(milestone, commission)`; a vintage can only be invested in its own commission year (`units_invest_max_period` 0 in the other periods, which maps back to `investable = false`). |
| `unit.decommission_date`, `link.decommission_date`, `node.storage_decommission_date` | existing units are 0 from the decommission year on (back: shows up only in `units_existing` per period) |
| `unit.time_resolution`, `node.time_resolution`, `link.time_resolution`, `unit__to_node.time_resolution`, `node__to_unit.time_resolution` | `assets_rep_periods_partitions` (unit and node) and `flows_rep_periods_partitions` (unit flows and link flows), block length = time_resolution / model resolution. A Duration gives a `uniform` partition; an Array of Durations gives a `math` partition, e.g. `[4h,4h,6h,6h,6h]` at 1 h <-> `2x4+3x6` (back: `explicit` and `math` partitions become an Array of Durations, constant ones a Duration). A flow time_resolution overrides the unit's for that flow (back: written on a unit flow only where it differs from its unit) |
| `investment_method`, `units_invest_max/min_period`, `units_max/min_cumulative`, `retirement_method` | `investable`, `investment_max/min_limit` (units x capacity), `max/min_available_units`, `decommissionable` |
| `lifetime`, `interest_rate`, `investment_uses_integer`, `investment_cost`, `fixed_cost`, `other_operational_cost`, `conversion_coefficient` | `technical_lifetime` + `economic_lifetime`, `discount_rate`, `investment_integer`, `investment_cost`, `fixed_cost`, `operational_cost`, `conversion_coefficient` |
| `startup_method`, `min_uptime`, `min_downtime` (minutes), `online_cost` | `unit_commitment` (basic, 3var when min up/down times are given), `unit_commitment_integer`, `minimum_up/down_time` (hours), `units_on_cost` |
| `ramp_method`, `ramp_limit_up/down` (p.u./minute) | `ramping`, `max_ramp_up/down` (p.u./hour) |
| `unit.availability` x output `profile_limit_upper` (`profile_method` upper_limit) | `availability` profile |
| `link` between two non-storage nodes | transport flow: `capacity`, `links_existing` -> export and import units, investment/retirement, cost, lifetime, `operational_cost`, `availability` profile |
| `link` between a node and a storage | two flows, storage power `capacity` = link capacity, `links_existing` -> `initial_units`, link investment -> storage power investment, link efficiencies -> charging/discharging efficiency |
| `link.transfer_method = exact_integer` on a link between a storage and a node | `asset.use_binary_storage_method = binary` (no simultaneous charging and discharging); Tulipa `relaxed_binary` is also written back as `exact_integer`. Back: a binary storage whose flows are not one two-way connection to a single node (e.g. charged directly by a unit and discharged to another node) gets a balance node `<storage>__bus`; all its flows move to the bus and the bus is joined to the storage by one two-way `exact_integer` link with the storage power capacity and efficiencies. Power can then also pass through the bus without entering the storage. Not applied when a storage flow is transport or has `storage_coefficient` other than 1. |
| `unit__to_node` into a storage / `node__to_unit` out of a storage (direct unit-storage flow) | Tulipa applies the storage charging/discharging efficiency and power capacity to every storage flow, units included. INES flow G is measured at the storage, Tulipa flow F at the unit: G = k F, k = charging efficiency (into storage) or 1 / discharging efficiency. To INES: the unit efficiency is multiplied by the storage efficiency (if this is the unit's only output / input, otherwise `conversion_coefficient` / k), the flow capacity is the stricter of the unit and storage capacity (storage capacity shared over the unit's existing units) times k, `other_operational_cost` (and investment/fixed cost if it is the main flow) / k. Back: the inverse; a secondary capacity of such a flow becomes the storage power capacity when the storage has no link (Tulipa has one power capacity per storage, a flow limit below it is reported) |
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
  year (`period.duration` is an array of their durations, weights in `representative_period_mapping`); values that
  vary per milestone year become maps over the INES periods.
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
| order of the timeframe periods in `rep_periods_mapping` (only the total weight per representative period is kept) | INES maps representative periods to periods with a total weight; the sequence is only needed for inter-period constraints, which are not mapped. |
| `stochastic_scenario`, `rep_periods_mapping.scenario`, `model_parameters.risk_aversion_*` | INES stochastic structure (forecasts) is different; no CVaR. |
| `asset.economic_lifetime` / `flow.economic_lifetime` when different from technical lifetime | INES has one `lifetime` (technical lifetime is used). |
| Rows of commission years that are not milestone years for storages, aggregated-method assets and transport flows | INES has no vintages for these; existing units of several commission years are summed. |
| `asset.use_inter_period_constraints` = false when there are representative periods, `inter_period_storage_level_bounds`, `assets_timeframe_*`, `profiles_timeframe`, `asset_milestone.min/max_energy_timeframe_partition` | No inter-period data in INES; INES -> Tulipa sets `use_inter_period_constraints` = true for storages when there are representative periods. |
| `assets_rep_periods_partitions`, `flows_rep_periods_partitions` that differ between periods or representative periods | INES has one `time_resolution` (Duration or Array of Durations) per unit, node, link or unit flow, the same for all periods. |
| `asset.use_binary_storage_method` on a storage with a transport flow or a flow with `storage_coefficient` other than 1; `energy_to_power_ratio` on a storage without a link | No INES counterpart (binary storage needs an `exact_integer` link, the energy-to-power ratio a unit-count constraint between storage and link). |
| storage `capacity` (power) when the storage has no link to a consumer/storage | INES storage nodes have no power capacity (it is in the connected units). |
| storage power capacity as a limit on a single unit flow, when it is below the storage power capacity from links | Tulipa has one power capacity per storage (limits the sum of its flows). |
| `flow.carrier` of a slack (penalty) flow, or a link whose two directions have different carriers | INES has the carrier on unit flows and on `node__link__node` only (one per link). |
| `flow_commission.storage_coefficient` other than 0 or 1; `capacity_coefficient`/`conversion_coefficient` on node-to-node flows | INES links have no coefficients (a storage outflow with `storage_coefficient` 0 becomes a separate unit). |
| `flow_both.initial_import_units` different from export units | INES links have one unit count for both directions. |
| `flow_milestone.dc_opf`, `reactance`, `model_parameters.power_system_base` | INES has no reactance. |
| `flows_relationships` involving a node-to-node flow | INES flow relationships are between unit flows only. |
| investment groups whose coefficients are not the asset capacities, or with non-unit members | INES set limits are in MW of unit capacity. |
| `node__to_unit.capacity_coefficient`, or any `capacity_coefficient` without `unit.combined_capacity` | Tulipa capacity limits only outputs, through the asset capacity. |
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

All converted datasets were solved with TulipaEnergyModel 0.23.2 + HiGHS. Because INES keeps only the total weight of
each representative period and one lifetime, each Tulipa case is also solved as a "reference" copy where each
representative period is mapped from one timeframe period with its total weight, inter-period profiles are removed,
storages use inter-period constraints (the INES -> Tulipa default when there are representative periods) and
economic lifetime = technical lifetime. Objective values:

| Case | Original | Reference | Tulipa -> INES -> Tulipa | Cause of remaining difference |
|---|---|---|---|---|
| Tiny | 278067.2 | 62487.59 | 62487.59 | - |
| UC-ramping | 302419.5 | 63970.99 | 63970.99 | - |
| tutorial-1 | 2.17577e8 | 2.17577e8 | 2.17577e8 | - |
| tutorial-2 / tutorial-3 | 1.48541e8 | 1.48541e8 | 1.48541e8 | - |
| tutorial-6-simple-method | 8.51171e6 | 8.42443e6 | 8.42443e6 | - |
| Variable Resolution | 28.4587 | 28.4587 | 28.4587 | - |
| Storage | 4498.4 | 4445.27 | 4446.55 | 0.03 %, not analysed (candidate: `caes` charging limit now on the energy entering the storage) |
| Norse | 2.52499e8 | 2.47484e7 | 2.39961e7 | not yet analysed; candidates: `Midgard_Hydro` with several links summed into one power capacity, the spillage flow's `capacity_coefficient`, the bus of the binary `Asgard_Battery` |
| Multi-year Investments | 4.63370e6 | 3.74155e6 | 3.74160e6 | MIP gap (integer investments) |
| INES test model | - | - | 4.770372e7 (INES -> Tulipa) vs 4.770367e7 (after INES -> Tulipa -> INES -> Tulipa) | MIP gap (binary storage); same constraints written with different scaling |

The INES certification cases all solve (storage 12.5 matches a hand calculation: 20 MWh missing from the renewable is
delivered through the storage at 2.5 /MWh).
