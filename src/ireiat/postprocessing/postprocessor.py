import logging
import os
import pickle
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import pyogrio

from ireiat.config.constants import CACHE_PATH

logger = logging.getLogger(__name__)


class PostProcessor:
    """Generate output artifacts from TAP solution files"""

    _tap_solution_path: Path
    _strongly_connected_graph_path: Path
    _shp_file_path: Path
    _solution_output_artifacts: Path = None
    _congestion_png_path: Path = None

    @property
    def artifact_output_path(self) -> Path:
        """Location in local filesystem for solution output artifacts"""
        if self._solution_output_artifacts is None:
            self._solution_output_artifacts = CACHE_PATH / "solution_output_artifacts"
            os.makedirs(self._solution_output_artifacts, exist_ok=True)
        return self._solution_output_artifacts

    @property
    def congestion_png_filename(self) -> str:
        return self._tap_solution_path.stem + ".png"

    def __init__(
        self,
        tap_solution_path: Path,
        strongly_connected_graph_path: Path,
        shp_file_path: Path,
    ):
        self._tap_solution_path = Path(tap_solution_path)
        self._strongly_connected_graph_path = strongly_connected_graph_path
        self._shp_file_path = shp_file_path

    def _generate_congestion_png(self):
        logger.info("Reading solution and graph data")
        traffic = pd.read_parquet(self._tap_solution_path)
        for cast_to_int_column in ["from", "to"]:
            traffic[cast_to_int_column] = traffic[cast_to_int_column].astype(int)

        # load the graph which tracks original faf_link_ids
        with open(self._strongly_connected_graph_path, "rb") as fp:
            strongly_connected_graph = pickle.load(fp)

        # we need to be careful about the order here. the solution (of edges) may be in a different order,
        # so we need to get the original_id of the edge on the network...and then join that data
        # into the solution
        edge_to_shp_file_link_id = {
            (es.source, es.target): es["original_id"] for es in strongly_connected_graph.es
        }

        # map the edge (source_vertex, destination_vertex) to the original shp edge id, which is stored in the graph
        shp_link_ids = [
            edge_to_shp_file_link_id[(row._1, row.to)]
            for row in traffic[["from", "to"]].itertuples()
        ]

        traffic["shp_link_id"] = shp_link_ids

        # this is a bit of a fudge in that the utilization on the same road (2 way) could be far above the capacity...
        # ideally we would set the capacity on each directed segment and sum the flows and capacities and then divide...
        logger.info("Computing utilization")
        traffic["utilization"] = traffic["flow"] / traffic["capacity"]
        grouped_traffic = traffic.groupby("shp_link_id")[["utilization"]].sum()

        # read the network
        logger.info("Reading network data")
        shp_file_gdf = pyogrio.read_dataframe(self._shp_file_path, use_arrow=True)
        if "id" not in shp_file_gdf.columns:
            if "FRAARCID" in shp_file_gdf.columns:
                shp_file_gdf["id"] = shp_file_gdf["FRAARCID"]
                logger.info("Assuming running rail postprocessing")
            else:
                shp_file_gdf["id"] = shp_file_gdf.index
                logger.info("Assuming running highway postprocessing")
        else:
            shp_file_gdf = shp_file_gdf.rename({"ID": "id"}, axis="columns")
            logger.info("Assuming running marine postprocessing")
        flows_with_geometry = (
            shp_file_gdf[["id", "geometry"]].set_index("id").join(grouped_traffic, how="left")
        )
        flows_with_geometry["utilization"] = flows_with_geometry["utilization"].fillna(0)

        # all grouped traffic (and associated edges) are found within the FAF data
        # assert len(flows_with_geometry) == len(grouped_traffic)

        non_zero_utilization = flows_with_geometry.loc[flows_with_geometry["utilization"] > 0]
        logger.info(non_zero_utilization.describe())
        logger.info("Generating plots for congestion")
        fig, ax = plt.subplots(figsize=(15, 8))
        flows_with_geometry.plot(ax=ax, color="grey", alpha=0.2)
        non_zero_utilization.plot(ax=ax, colors=plt.cm.YlOrRd(non_zero_utilization["utilization"]))
        ax.set_xlim(-130, -60)
        ax.set_ylim(23, 50)
        plt.tight_layout()
        plt.savefig(self.artifact_output_path / self.congestion_png_filename)
        logger.info(f"Plots saved to {self.artifact_output_path}")

    def postprocess(self):
        # create a graph of the traffic image
        self._generate_congestion_png()
