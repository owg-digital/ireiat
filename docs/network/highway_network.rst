===============
Highway network
===============

Generating a highway network to be used in the TAP requires several steps:

* Parse FAF5 (undirected) highway link data
* Translate FAF5 (undirected) links into a directed graph (nodes and edges)
* Limit the graph to a strongly connected graph
* Keep track of the index and spatial attributes of nodes and edges on the strongly connected graph
* Construct a ball tree for the nodes on the strongly connected graph
