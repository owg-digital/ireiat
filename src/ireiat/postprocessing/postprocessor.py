import logging
import os
import pickle
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import pyogrio

from ireiat.config import CACHE_PATH

logger = logging.getLogger(__name__)


class PostProcessor:
    """Generate output artifacts from TAP solution files"""

    _tap_solution_path: Path
    _strongly_connected_graph_path: Path
    _faf5_highway_network_path: Path
    _solution_output_artifacts: Path = None

    @property
    def artifact_output_path(self) -> Path:
        """Location in local filesystem for solution output artifacts"""
        if self._solution_output_artifacts is None:
            self._solution_output_artifacts = CACHE_PATH / "solution_output_artifacts"
            os.makedirs(self._solution_output_artifacts, exist_ok=True)
        return self._solution_output_artifacts

    def __init__(
        self,
        tap_solution_path: Path,
        strongly_connected_graph_path: Path,
        faf5_highway_network_path: Path,
    ):
        self._tap_solution_path = tap_solution_path
        self._strongly_connected_graph_path = strongly_connected_graph_path
        self._faf5_highway_network_path = faf5_highway_network_path

    def _generate_congestion_png(self):
        logger.info("Reading solution and graph data")
        traffic = pd.read_parquet(self._tap_solution_path)
        for cast_to_int_column in ["from", "to"]:
            traffic[cast_to_int_column] = traffic[cast_to_int_column].astype(int)

        # load the graph which tracks original faf_link_ids
        with open(self._strongly_connected_graph_path, "rb") as fp:
            strongly_connected_highway_graph = pickle.load(fp)

        # we need to be careful about the order here. the solution (of edges) may be in a different order,
        # so we need to get the original_faf_link_id of the edge on the network...and then join that data
        # into the solution
        edge_to_faf_link_id = {
            (es.source, es.target): es["original_id"] for es in strongly_connected_highway_graph.es
        }

        # map the edge (source_vertex, destination_vertex) to the original faf edge id, which is stored in the graph
        faf_link_ids = [
            edge_to_faf_link_id[(row._1, row.to)] for row in traffic[["from", "to"]].itertuples()
        ]

        traffic["faf_link_id"] = faf_link_ids

        # this is a bit of a fudge in that the utilization on the same road (2 way) could be far above the capacity...
        # ideally we would set the capacity on each directed segment and sum the flows and capacities and then divide...
        logger.info("Computing utilization")
        traffic["utilization"] = traffic["flow"] / traffic["capacity"]
        grouped_traffic = traffic.groupby("faf_link_id")[["utilization"]].sum()

        # read the network
        logger.info("Reading network data")
        faf5_links_gdf = pyogrio.read_dataframe(self._faf5_highway_network_path, use_arrow=True)

        flows_with_geometry = (
            faf5_links_gdf[["id", "geometry"]].set_index("id").join(grouped_traffic, how="inner")
        )

        # all grouped traffic (and associated edges) are found within the FAF data
        assert len(flows_with_geometry) == len(grouped_traffic)

        non_zero_utilization = flows_with_geometry.loc[flows_with_geometry["utilization"] > 0]
        logger.info("Generating plots for congestion")
        fig, ax = plt.subplots(figsize=(15, 8))
        flows_with_geometry.plot(ax=ax, color="grey", alpha=0.2)
        non_zero_utilization.plot(ax=ax, colors=plt.cm.YlOrRd(non_zero_utilization["utilization"]))
        ax.set_xlim(-1.4e7, -0.7e7)
        ax.set_ylim(2.5e6, 6.5e6)
        plt.tight_layout()
        plt.savefig(self.artifact_output_path / "traffic.png")
        logger.info(f"Plots saved to {self.artifact_output_path}")

    def postprocess(self):
        # create a graph of the traffic image
        self._generate_congestion_png()
