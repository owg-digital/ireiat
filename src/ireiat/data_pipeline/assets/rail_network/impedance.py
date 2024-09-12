from itertools import chain, product
from typing import Tuple, Set, Dict, Any

import igraph as ig

from ireiat.config import RAIL_DEFAULT_MPH_SPEED
from ireiat.util.rail_network_constants import EdgeType


def _generate_subgraphs(g: ig.Graph, separation_attribute: str = "owner") -> Dict[str, ig.Graph]:
    """
    Returns subgraphs by unique entries in the `separation_attribute`  for the graph.
    Returned subgraphs have an 'original_idx' field with the format {X}{Y}, where X is the
    `separation_attribute` string and Y is the original vertex index.
    E.g. 0->1 (Graph A) would have ['A0','A1'] vertex attributes

    Additionally, each subgraph's `separation_attribute` now takes the string of the attribute value
    to which the subgraph belongs. E.g. Graph with 0->1 (separation_attribute={'A','B'} would have
    two graphs returned, each with `separation_attribute='A'` or `separation_attribute='B'`.

    :param g: graph to separate by a given attribute
    :return: Dict of subgraphs, keyed by unique separation attribute values"""
    # create subgraphs by owner
    unique_owners_across_edges = set(chain.from_iterable(g.es[separation_attribute]))
    edge_list_by_owner: Dict[str, Set[int]] = {k: set() for k in unique_owners_across_edges}
    for e in g.es:
        for k in e[separation_attribute]:
            edge_list_by_owner[k].add(e.index)
    # retain all vertices in the subgraph to preserve numbering
    subgraphs = {
        k: g.subgraph_edges(v, delete_vertices=False) for k, v in edge_list_by_owner.items()
    }
    for k, subgraph in subgraphs.items():
        subgraph.es[separation_attribute] = [f"{k}" for _ in subgraph.es]
        subgraph.vs["original_idx"] = [f"{k}{v.index}" for v in subgraph.vs]
    return subgraphs


def _generate_impedances(g: ig.Graph, separation_attribute="owner") -> Set[Tuple[str, str]]:
    """
    Given a graph with a `impedance_attribute` on each edge, determine the impedance
    edges that would be needed to join subgraphs that were created from unique values of the
    impedance attribute. For example, if a graph of 0 -> 1 -> 2 had 'owners' 'A' on the first
    edge and 'B' on the second edge, this method would return `{('A1','B1')}` given that a
    graph split by owner would be A graph: 0->1 and B graph: 1->2 and there would be an
    impedance edge between 'A1' and 'B1'.

    :param g: graph to generate impedance edges from
    :param separation_attribute: string identifer on the graph edges
    :return: Set of tuples of impedance graph edges
    """
    impedances = set()
    for v in g.vs:
        in_edges, out_edges = v.in_edges(), v.out_edges()
        # cater for single edges
        if len(out_edges) == 1 and len(in_edges) == 1:
            in_owners, out_owners = (
                in_edges[0][separation_attribute],
                out_edges[0][separation_attribute],
            )
            if in_owners == out_owners:
                continue  # we have single in/out edges with the same owner on each side
            else:
                # we have single in/out edges with different owners
                for in_edge_owner, out_edge_owner in product(in_owners, out_owners):
                    if in_edge_owner != out_edge_owner:
                        impedances.add((f"{in_edge_owner}{v.index}", f"{out_edge_owner}{v.index}"))
        else:
            for in_edge in in_edges:
                in_edge_owners = in_edge[separation_attribute]
                for out_edge in out_edges:
                    out_edge_owners = out_edge[separation_attribute]
                    for in_edge_owner, out_edge_owner in product(in_edge_owners, out_edge_owners):
                        if in_edge_owner != out_edge_owner:
                            impedances.add(
                                (f"{in_edge_owner}{v.index}", f"{out_edge_owner}{v.index}")
                            )

    return impedances


def generate_impedance_graph(g: ig.Graph, separation_attribute="owner") -> ig.Graph:
    """Given a graph whose edges all contain `separation_attribute`, return an "exploded"
    graph with impedances edges between vertices that have different values of the
    separation attribute. See the detailed test cases for how this method is intended to function.

    :param g: iGraph with `separation_attribute` of type `Set`
    :param separation_attribute: string identifying the attribute
    :return: an exploded graph with impedance edges

    """
    impedances = _generate_impedances(g, separation_attribute=separation_attribute)
    subgraphs = _generate_subgraphs(g, separation_attribute=separation_attribute)
    disjoint_union = ig.disjoint_union(subgraphs.values())
    disjoint_union_vertex_mapping = {v["original_idx"]: v.index for v in disjoint_union.vs}
    impedance_edges = [
        (disjoint_union_vertex_mapping[from_id], disjoint_union_vertex_mapping[to_id])
        for from_id, to_id in impedances
    ]
    # construct default edge attributes for impedance edges and add them
    impedance_edge_attrs: Dict[str, Any] = dict()
    impedance_edge_attrs["speed"] = [RAIL_DEFAULT_MPH_SPEED for _ in impedance_edges]
    impedance_edge_attrs["edge_type"] = [EdgeType.IMPEDANCE_LINK.value for _ in impedance_edges]
    impedance_edge_attrs[separation_attribute] = ["imp" for _ in impedance_edges]
    disjoint_union.add_edges(impedance_edges, impedance_edge_attrs)

    # eliminate zero degree vertices, preserved when creating subgraphs
    disjoint_union.delete_vertices(disjoint_union.vs.select(_degree=0))
    return disjoint_union
