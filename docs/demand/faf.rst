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
4. **Other & Unknown**: ``faf5_other_and_unknow_demand``

Each dataset represents the total tons of demand between origin-destination pairs for that specific mode of transportation.

Currently, there are no configurable CLI options for the data pipeline.

.. automodule:: ireiat.util.faf_constants
    :members:

.. autodata:: ireiat.config.NON_CONTAINERIZABLE_COMMODITIES
