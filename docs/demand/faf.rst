Demand
======

We use FAF to source all origin-destination demand for "containerizable"
freight. The list of excluded commodity codes is detailed in
:data:`ireiat.config.NON_CONTAINERIZABLE_COMMODITIES`.

The FAF dataset is subset by filtering out non-containerizable commodities.
Once the containerizable demand is isolated, we further divide the data by transportation
mode to create separate demand datasets for each mode of transportation.
These modes are:

1. **Truck**: ``faf5_truck_demand``
2. **Rail**: ``faf5_rail_demand``
3. **Water**: ``faf5_water_demand``

Handling of Other & Unknown Modes
=================================
Rows with Other & Unknown modes are distributed proportionally across the Truck, Rail, and Water
datasets based on the relative tonnage of known modes (truck, rail, water) for each origin-destination pair.
This proportional allocation ensures that demand for unknown modes is captured according to the
transportation patterns of known modes.

Example Process
===============
1. The dataset is first split into known modes (truck, rail, water) and unknown modes.
2. For each origin-destination pair, the total demand for truck, rail, and water is calculated.
3. Unknown mode demand is then allocated across truck, rail, and water in proportion to their existing tonnage for each origin-destination pair.
4. Finally, the recalculated demand for truck, rail, and water is aggregated to produce the final datasets.

Each dataset represents the total tons of demand between origin-destination pairs for that specific mode of transportation,
including any reassigned demand from unknown modes.

Currently, there are no configurable CLI options for the data pipeline.

.. automodule:: ireiat.util.faf_constants
    :members:

.. autodata:: ireiat.config.NON_CONTAINERIZABLE_COMMODITIES
