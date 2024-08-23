import unittest

import igraph as ig

from ireiat.data_pipeline.assets.rail_network.impedance import (
    generate_impedance_graph,
    _generate_subgraphs,
)


class TestImpedances(unittest.TestCase):

    def test_generate_subgraphs_works_for_a_simple_case(self):
        g = ig.Graph(directed=True)
        g.add_vertices(3)
        g.add_edges([(0, 1), (1, 2)])
        g.es["owner"] = [{"A", "B"}, {"B"}]
        s = _generate_subgraphs(g)
        self.assertEqual(len(s.keys()), 2)  # just A and B
        a_graph, b_graph = s["A"], s["B"]
        self.assertEqual(len(a_graph.es), 1)  # A graph has 1 edge
        self.assertEqual(len(b_graph.es), 2)  # B graph has 2 edges

    def _node_edge_impedance_confirmation(
        self, g: ig.Graph, expected_nodes: int, expected_edges: int, expected_impedance_edges: int
    ):
        """Helper method used within this test suite"""
        i = generate_impedance_graph(g)
        self.assertEqual(len(i.vs), expected_nodes)  # should have expected nodes
        self.assertEqual(len(i.es), expected_edges)  # should have expected edges
        self.assertEqual(
            len(i.es.select(owner_eq="imp")), expected_impedance_edges
        )  # should have expected impedance edge, relies on 'owner' being the separation attribute

    def test_simple_graph_establishes_sensible_impedances(self):
        g = ig.Graph(directed=True)
        g.add_vertices(4)
        g.add_edges([(0, 1), (1, 2), (2, 3)])
        g.es["owner"] = [{"A", "B"}, {"A", "B"}, {"B"}]
        self._node_edge_impedance_confirmation(g, 7, 6, 1)

    def test_junction_with_two_outlet_owners_establishes_sensible_impedances(self):
        g = ig.Graph(directed=True)
        g.add_vertices(4)
        g.add_edges([(0, 1), (1, 2), (1, 3)])
        g.es["owner"] = [{"A"}, {"A"}, {"C"}]
        self._node_edge_impedance_confirmation(g, 5, 4, 1)

    def test_junction_with_two_inlet_and_two_outlet_establishes_impedances(self):
        g = ig.Graph(directed=True)
        g.add_vertices(5)
        g.add_edges([(0, 1), (1, 2), (3, 1), (1, 4)])
        g.es["owner"] = [{"A"}, {"A"}, {"C"}, {"C"}]
        self._node_edge_impedance_confirmation(g, 6, 6, 2)

    def test_bi_directional_junction_with_two_inlet_and_two_outlet_establishes_impedances(self):
        g = ig.Graph(directed=True)
        g.add_vertices(5)
        g.add_edges([(0, 1), (1, 0), (1, 2), (2, 1), (3, 1), (1, 3), (1, 4), (4, 1)])
        g.es["owner"] = [{"A"}, {"A"}, {"C"}, {"C"}]
        self._node_edge_impedance_confirmation(g, 6, 10, 2)

    def test_multiple_owners_establishes_impedances(self):
        g = ig.Graph(directed=True)
        g.add_vertices(5)
        g.add_edges([(0, 1), (1, 2), (2, 3), (2, 4)])
        g.es["owner"] = [{"A", "B"}, {"B"}, {"A", "B"}, {"C"}]
        self._node_edge_impedance_confirmation(g, 10, 9, 3)
