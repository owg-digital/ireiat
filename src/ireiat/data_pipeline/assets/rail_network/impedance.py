from itertools import chain, product
from typing import Set, Dict, Any

import igraph as ig
import numpy as np
from sklearn.neighbors import BallTree

from ireiat.config.constants import RADIUS_EARTH_MILES
from ireiat.config.data_pipeline import RailImpedanceConfig, GeographicImpedance
from ireiat.config.rail_enum import EdgeType
from ireiat.data_pipeline.assets.rail_network import SEPARATION_ATTRIBUTE_NAME


def _generate_subgraphs(
    g: ig.Graph, separation_attribute: str = SEPARATION_ATTRIBUTE_NAME
) -> Dict[str, ig.Graph]:
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
    subgraphs = {k: g.subgraph_edges(v) for k, v in edge_list_by_owner.items()}
    for k, subgraph in subgraphs.items():
        subgraph.es[separation_attribute] = [f"{k}" for _ in subgraph.es]
    return subgraphs


def _generate_impedances(
    g: ig.Graph, separation_attribute=SEPARATION_ATTRIBUTE_NAME
) -> set[tuple[str, tuple[float, float], str, tuple[float, float]]]:
    """
    Given a graph with a `separation_attribute` on each edge, determine the impedance
    edges that would be needed to join subgraphs that were created from unique values of the
    separation attribute. For example, if a graph of 0 -> 1 -> 2 had 'owners' 'A' on the first
    edge and 'B' on the second edge (along with origin and destination coords for each edge),
    this method would return `{('A', destination_coords, 'B', origin_coords)}` given
    that a graph split by owner would be A graph: 0->1 and B graph: 1->2 and there would be an
    impedance edge between 'A1' and 'B1' (with appropriate coordinates returned).

    :param g: graph to generate impedance edges from
    :param separation_attribute: string identifer on the graph edges
    :return: Set of tuples of impedance graph edges
    """
    impedances = set()
    for v in g.vs:
        in_edges, out_edges = v.in_edges(), v.out_edges()
        if len(out_edges) == 1 and len(in_edges) == 1:
            in_owners, out_owners = (
                in_edges[0][separation_attribute],
                out_edges[0][separation_attribute],
            )
            if in_owners == out_owners:
                continue  # we have single in/out edges with the same owner on each side
            else:
                # we have single in/out edges with different owners
                in_edge_destination_coords = in_edges[0]["destination_coords"]
                out_edge_origin_coords = out_edges[0]["origin_coords"]
                for in_edge_owner, out_edge_owner in product(in_owners, out_owners):
                    if in_edge_owner != out_edge_owner:
                        impedances.add(
                            (
                                in_edge_owner,
                                in_edge_destination_coords,
                                out_edge_owner,
                                out_edge_origin_coords,
                            )
                        )
        else:
            for in_edge in in_edges:
                in_edge_destination_coords = in_edge["destination_coords"]
                in_edge_owners = in_edge[separation_attribute]
                for out_edge in out_edges:
                    out_edge_origin_coords = out_edge["origin_coords"]
                    out_edge_owners = out_edge[separation_attribute]
                    for in_edge_owner, out_edge_owner in product(in_edge_owners, out_edge_owners):
                        if in_edge_owner != out_edge_owner:
                            impedances.add(
                                (
                                    in_edge_owner,
                                    in_edge_destination_coords,
                                    out_edge_owner,
                                    out_edge_origin_coords,
                                )
                            )

    return impedances


def _get_geographic_owner_impedance_map(
    override: GeographicImpedance,
) -> dict[tuple[str, str], int]:
    override_map = dict()
    for rail_override in override.overrides:
        override_map[(rail_override.from_owner, rail_override.to_owner)] = rail_override.impedance
        override_map[(rail_override.to_owner, rail_override.from_owner)] = rail_override.impedance
    return override_map


def _override_impedances_with_geographic_config(
    computed_impedances: list,
    impedances: set,
    overrides: list[GeographicImpedance],
    class_1_rr_codes,
):
    """Constructs a BallTree from all impedances (using the approximate midpoint of the impedance link
    origin/destination lat/longs) and, for each geographic override, uses the geographic override's impedance
    values as applicable"""
    if len(overrides) == 0:
        return computed_impedances

    # construct a ball tree for all impedance links from midpoints, as well as lists for later lookup
    impedance_link_approx_lat_long: list[tuple[float, float]] = []
    impedance_owners: list[tuple[str, str]] = []
    for src_owner, dest_coords, dest_owner, origin_coords in impedances:
        dest_lat, dest_long = dest_coords
        origin_lat, origin_long = origin_coords
        approx_lat = (dest_lat + origin_lat) / 2
        approx_long = (dest_long + origin_long) / 2
        impedance_link_approx_lat_long.append((approx_lat, approx_long))
        impedance_owners.append((src_owner, dest_owner))
    impedance_link_radians = np.deg2rad(np.array(impedance_link_approx_lat_long))
    bt = BallTree(impedance_link_radians, metric="haversine")

    # cycle through the set of geographic overrides
    for geographic_impedance in overrides:
        owner_impedance_map = _get_geographic_owner_impedance_map(geographic_impedance)
        override_radians = np.deg2rad(
            [(geographic_impedance.latitude, geographic_impedance.longitude)]
        )
        radius_radians = geographic_impedance.radius_miles / RADIUS_EARTH_MILES
        idxs, distances = bt.query_radius(override_radians, r=radius_radians, return_distance=True)

        # for each impedance link within the search radius, test whether it has a custom override, else use defaults
        for override_idx in idxs[0]:
            link_impedance_owners: tuple[str, str] = impedance_owners[override_idx]
            if link_impedance_owners in owner_impedance_map:
                computed_impedances[override_idx] = owner_impedance_map[link_impedance_owners]
            else:
                if all([o in class_1_rr_codes for o in link_impedance_owners]):
                    computed_impedances[override_idx] = (
                        geographic_impedance.class_1_to_class_1_impedance
                    )
                else:
                    computed_impedances[override_idx] = geographic_impedance.default_impedance

    return computed_impedances


def generate_impedance_values(impedances: set, config: RailImpedanceConfig | None = None):
    """Looks up impedance values given the configuration passed, which can be geographic or generic"""
    if config is None:
        return [250 for _ in impedances]

    impedance_vals = []
    class_1_rr_codes = set(config.class_1_rr_codes)
    for src_owner, dest_coords, dest_owner, origin_coords in impedances:
        if src_owner in class_1_rr_codes and dest_owner in class_1_rr_codes:
            impedance_vals.append(config.class_1_to_class_1_impedance)
        else:
            impedance_vals.append(config.default_impedance)
    return _override_impedances_with_geographic_config(
        impedance_vals, impedances, config.geographic_overrides, class_1_rr_codes
    )


def generate_impedance_graph(
    g: ig.Graph,
    separation_attribute=SEPARATION_ATTRIBUTE_NAME,
    config: RailImpedanceConfig | None = None,
) -> ig.Graph:
    """Given a graph whose edges all contain `separation_attribute`, return an "exploded"
    graph with impedances edges between vertices that have different values of the
    separation attribute. See the detailed test cases for how this method is intended to function.

    :param g: iGraph with `separation_attribute` of type `Set`
    :param separation_attribute: string identifying the attribute
    :param config: rail configuration when running the data pipeline
    :return: an exploded graph with impedance edges

    """
    impedances = _generate_impedances(g, separation_attribute=separation_attribute)
    print(f"Generated {len(impedances)} impedances")
    subgraphs = _generate_subgraphs(g, separation_attribute=separation_attribute)
    print(f"Generated {len(subgraphs)} subgraphs")
    disjoint_union = ig.disjoint_union(subgraphs.values())

    # cache vertices from/to in the disjoint graph to facilitate fast lookups
    vertices_from = {
        (e[separation_attribute], e["destination_coords"]): e.target_vertex.index
        for e in disjoint_union.es
    }
    vertices_to = {
        (e[separation_attribute], e["origin_coords"]): e.source_vertex.index
        for e in disjoint_union.es
    }
    impedance_edges = [
        (vertices_from[(src_owner, dest_coords)], vertices_to[(dest_owner, origin_coords)])
        for src_owner, dest_coords, dest_owner, origin_coords in impedances
    ]

    # construct default edge attributes for impedance edges and add them
    impedance_edge_attrs: Dict[str, Any] = dict()
    impedance_edge_attrs["edge_type"] = [EdgeType.IMPEDANCE_LINK.value for _ in impedance_edges]
    impedance_edge_attrs[separation_attribute] = ["imp" for _ in impedance_edges]
    impedance_edge_attrs["length"] = generate_impedance_values(impedances, config)
    disjoint_union.add_edges(impedance_edges, impedance_edge_attrs)

    # eliminate zero degree vertices, preserved when creating subgraphs
    disjoint_union.delete_vertices(disjoint_union.vs.select(_degree=0))
    return disjoint_union
