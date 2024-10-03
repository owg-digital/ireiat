import logging
import os
import shutil
import subprocess
from importlib import resources
from pathlib import Path
from typing import Optional

import click

from ireiat import r_source
from ireiat.config.constants import CACHE_PATH
from ireiat.config.runtime_config import (
    run_config_map,
    RunConfig,
    PostprocessConfig,
    postprocess_config_map,
)
from ireiat.postprocessing.postprocessor import PostProcessor
from ireiat.util.logging_ import configure_logging

configure_logging(output_file=True)
logger = logging.getLogger(__name__)

MODE_CHOICES = click.Choice(["highway", "marine", "rail"])


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
)
@click.option(
    "--od-file",
    "-d",
    type=click.Path(exists=True),
    help="A parquet file that represents the demand to be used for the TAP",
)
@click.option(
    "--output-file",
    "-o",
    type=click.Path(),
    help="A parquet file to which the TAP results will be written",
)
@click.option(
    "--mode",
    "-m",
    type=MODE_CHOICES,
    help="If specified, uses defaults file outputs for the given mode unless other parameters are passed",
)
@click.option(
    "--max-gap", "-g", type=float, default=1e-8, help="The relative gap used in Algorithm B"
)
@click.option("--max-iterations", "-i", type=int, default=1, help="Max iterations for Algorithm B")
def solve(
    network_file: Optional[Path],
    od_file: Optional[Path],
    output_file: Optional[Path],
    mode: Optional[str],
    max_gap: float,
    max_iterations: int,
):
    """Runs the TAP solution in R using cppRouting"""

    config = run_config_map.get(mode, RunConfig)(
        passed_network_file_path=network_file,
        passed_od_file_path=od_file,
        passed_output_file_path=output_file,
    )

    # use the bundled 'tap.r' file as a "resource" and create a temporary file to be run by RScript
    temporary_file_path = CACHE_PATH / "local_tap.r"
    with open(temporary_file_path, "w") as tf:
        with resources.open_text(r_source, "tap.r") as r_text:
            tf.write(r_text.read())

    # pass the command for the RScript file
    cmd = [
        "Rscript",
        tf.name,
        config.network_file_path,
        config.od_file_path,
        str(max_gap),
        config.output_file_path,
        str(max_iterations),
    ]
    logger.info(f"Calling subprocess {cmd}")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    # Read the output line by line as it becomes available
    for line in process.stdout:
        print(line, end="")
    process.wait()

    temporary_file_path.unlink(missing_ok=True)


@cli.command()
@click.option(
    "--solution",
    "-s",
    type=click.Path(exists=True),
    help="Solution file representing assigned traffic",
)
@click.option(
    "--solution-graph",
    "-g",
    type=click.Path(exists=True),
    help="The strongly connected (pickled igraph) graph on which the problem has been solved",
)
@click.option(
    "--network-shp",
    "-n",
    type=click.Path(exists=True),
    help="The shp file used to represent the underlying network to enable visualization",
)
@click.option(
    "--mode",
    "-m",
    type=MODE_CHOICES,
    help="If specified, uses defaults file outputs for the given mode unless other parameters are passed",
)
def postprocess(solution: Path, solution_graph: Path, network_shp: Path, mode: str):
    """Post processes results and save in the default configured cache path"""
    logger.info("Running postprocessing.")
    config = postprocess_config_map.get(mode, PostprocessConfig)(
        passed_traffic_path=solution,
        passed_network_graph_path=solution_graph,
        passed_shp_file_path=network_shp,
    )

    pp = PostProcessor(config.traffic_file_path, config.network_graph_path, config.shp_file_path)
    pp.postprocess()


if __name__ == "__main__":
    cli()
