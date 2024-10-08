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
---------------------------------
Rows with Other & Unknown modes are distributed proportionally across the Truck, Rail, and Water
datasets based on the relative tonnage of known modes (truck, rail, water) for each origin-destination pair.
This proportional allocation ensures that demand for unknown modes is captured according to the
transportation patterns of known modes.

County-to-County Processing
---------------------------
FAF5 demand is projected from FAF regions into counties proportional to U.S. Census population data.
This projection assumes a uniform distribution of population across the area of each county,
which is a critical assumption that could be improved in future iterations.

.. note::
    There are other, more detailed options for projecting FAF demand into counties
    and/or localizing the demand. However, our data pipeline has opted for a simple,
    explainable process, especially for this initial release.

The key steps in the county-to-county processing workflow are as follows:

1. **Look Up Population Data**: The U.S. Census population data for each county is retrieved to serve as the basis for the demand projection.
2. **Compute the Area of Each County**: For each county within a given FAF region, the area is computed to enable proportional allocation of demand.
3. **Total Population Calculation**: The total population within each FAF region is computed, considering the uniform assumption of population distribution within counties.
4. **Allocate FAF Demand**: FAF demand is allocated into each county based on the proportion of the county's population relative to the total population of the FAF region.
5. **Localize Demand in County Centroids**: The demand for each county is localized at a single geographic point—the county centroid—where all demand originates and terminates for each mode of transportation.

This simple approach ensures that each county has a defined geographic point (the centroid)
where the demand originates or terminates, creating an easily explainable model for the allocation
of FAF5 demand at the county level.

Example Process
---------------
1. The dataset is first split into known modes (truck, rail, water) and unknown modes.
2. For each origin-destination pair, the total demand for truck, rail, and water is calculated.
3. Unknown mode demand is then allocated across truck, rail, and water in proportion to their existing tonnage for each origin-destination pair.
4. The recalculated demand for truck, rail, and water is aggregated to produce the final datasets.
5. Each mode's dataset is then processed at the county level, generating county-to-county tonnage records.

Summary
-------

Each dataset represents the total tons of demand between origin-destination pairs for that specific mode of transportation,
including any reassigned demand from unknown modes.

Currently, there are no configurable CLI options for the data pipeline.

.. automodule:: ireiat.config.faf_enum
    :members:

.. autodata:: ireiat.config.constants.NON_CONTAINERIZABLE_COMMODITIES
