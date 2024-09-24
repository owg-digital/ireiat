import logging
import os
import shutil
import subprocess
from importlib import resources
from pathlib import Path

import click

from ireiat import r_source
from ireiat.config import CACHE_PATH, INTERMEDIATE_PATH
from ireiat.postprocessing.postprocessor import PostProcessor
from ireiat.util.logging_ import configure_logging

configure_logging(output_file=True)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--debug/--no-debug", default=False)
def cli(debug):
    logger.info(f"Debug mode is {'on' if debug else 'off'}")
    os.makedirs(CACHE_PATH, exist_ok=True)


@cli.command()
def clear_cache():
    """Clears the local cache directory"""
    result: str = input("Are you sure you want to clear the cache? [y/n] ")
    if result.strip().lower() == "y":
        logger.info(f"Deleting files at {CACHE_PATH}")
        shutil.rmtree(CACHE_PATH)


@cli.command()
@click.option(
    "--network-file",
    "-n",
    type=click.Path(exists=True),
    help="A parquet file that represents the network to be used for the TAP",
    default=CACHE_PATH / INTERMEDIATE_PATH / "tap_highway_network_dataframe.parquet",
)
@click.option(
    "--od-file",
    "-d",
    type=click.Path(exists=True),
    help="A parquet file that represents the demand to be used for the TAP",
    default=CACHE_PATH / INTERMEDIATE_PATH / "tap_highway_tons.parquet",
)
@click.option(
    "--max-gap", "-g", type=float, default=1e-8, help="The relative gap used in Algorithm B"
)
def solve(network_file: Path, od_file: Path, max_gap: float):
    """Runs the TAP solution in R using cppRouting"""

    # use the bundled 'tap.r' file as a "resource" and create a temporary file to be run by RScript
    temporary_file_path = CACHE_PATH / "local_tap.r"
    with open(temporary_file_path, "w") as tf:
        with resources.open_text(r_source, "tap.r") as r_text:
            tf.write(r_text.read())

    # pass the command for the RScript file
    cmd = ["Rscript", tf.name, network_file, od_file, CACHE_PATH, str(max_gap)]
    logger.debug(f"About to run {cmd}")
    subprocess.call(cmd, shell=True, universal_newlines=True)
    temporary_file_path.unlink(missing_ok=True)


@cli.command()
@click.option(
    "--solution",
    "-s",
    type=click.Path(exists=True),
    help="Solution file representing assigned traffic",
    default=CACHE_PATH / "traffic.parquet",
)
@click.option(
    "--solution-graph",
    "-g",
    type=click.Path(exists=True),
    help="The strongly connected (pickled igraph) graph on which the problem has been solved",
    default=CACHE_PATH / INTERMEDIATE_PATH / "strongly_connected_highway_graph",
)
@click.option(
    "--network-shp",
    "-n",
    type=click.Path(exists=True),
    help="The shp file used to represent the underlying network to enable visualization",
    default=CACHE_PATH / "raw/faf5_highway_links.zip",
)
def postprocess(solution: Path, solution_graph: Path, network_shp: Path):
    """Post processes results and save in the default configured cache path"""
    logger.info("Running postprocessing.")
    pp = PostProcessor(solution, solution_graph, network_shp)
    pp.postprocess()


if __name__ == "__main__":
    cli()
