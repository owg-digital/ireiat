import unittest

import igraph
import pandas as pd

from ireiat.util.graph import generate_zero_based_node_maps, get_allowed_node_indices


class TestGIS(unittest.TestCase):

    def setUp(self) -> None:
        cols = [
            "origin_latitude",
            "origin_longitude",
            "destination_latitude",
            "destination_longitude",
        ]
        record_with_duplicate_origins = [(1, 1, 2, 2), (1, 1, 3, 3)]
        self.duplicate_origin_df = pd.DataFrame(record_with_duplicate_origins, columns=cols)
        record_with_duplicate_dest = [(1, 1, 2, 2), (3, 3, 2, 2)]
        self.duplicate_destination_df = pd.DataFrame(record_with_duplicate_dest, columns=cols)

    def test_generate_zero_nodes_with_duplicate_origin(self):
        dict_result = generate_zero_based_node_maps(self.duplicate_origin_df)
        self.assertEqual(len(dict_result), 3)
        self.assertEqual(max([v for v in dict_result.values()]), 2)  # max index = count-1

    def test_generate_zero_nodes_with_duplicate_destination(self):
        dict_result = generate_zero_based_node_maps(self.duplicate_destination_df)
        self.assertEqual(len(dict_result), 3)

    def test_fully_connected_graph_returns_all_node_indices(self):
        g = igraph.Graph(edges=[(1, 2), (2, 3)])
        result = get_allowed_node_indices(g)
        self.assertEqual(len(result), 3)  # nodes 1,2,3 all connected

    def test_partly_connected_graph_returns_biggest_connected_component(self):
        g = igraph.Graph(edges=[(1, 2), (2, 3), (5, 6)])
        result = get_allowed_node_indices(g)
        self.assertEqual(len(result), 3)  # nodes 1,2,3 all connected - nodes 5,6 not connected
