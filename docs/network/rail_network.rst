============
Rail network
============

The rail network is the most complex network within the project.

The network can be constructed from:

* NARN data
* Intermodal terminal data

But the NARN data alone is insufficient to construct a "realistic"
rail network because it does not account for interchange costs among
operators. To do so, we need to examine the railroad owners and railroad
operators with trackage rights on each edge.

Given each edge's set of owner/operators, we can create subgraphs of
the entire NARN graph by owner/operator (one subgraph for each operator)
and join the subgraphs with "impedance" edges that represent interchange
"costs."

..  image:: ../_static/network/rail/impedance_example.png

These are accomplished through:

.. automodule:: ireiat.data_pipeline.assets.rail_network.impedance
    :members:
    :private-members:

Note that because there are 900+ owner operators, we only use a subset - but cover ~80% of
the relevant rail network edges. Thus, we may not account for some short line interchange fees.

For intermodal facilities, we connect each facility to the impedance graph by adding two new vertices:

1. **Intermodal Terminal**: Represents the intermodal terminal.
2. **Intermodal Dummy Node**: Serves as an intermediary connection point for calculating intermodal capacities and costs.

These two nodes are linked by a pair of **Intermodal Capacity** edges. Each dummy node is then connected to existing
rail network nodes based on the terminal's railroad operators using **Rail to Quant** or **Quant to Rail** edges.

..  image:: ../_static/network/rail/intermodal_terminals_example.png
