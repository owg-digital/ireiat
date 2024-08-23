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
