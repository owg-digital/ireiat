import logging
import os
import subprocess
from importlib import resources
from pathlib import Path

import click

from ireiat import r_source
from ireiat.config import CACHE_PATH
from ireiat.util.logging_ import configure_logging

configure_logging(output_file=True)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--debug/--no-debug", default=False)
def cli(debug):
    logger.info(f"Debug mode is {'on' if debug else 'off'}")
    os.makedirs(CACHE_PATH, exist_ok=True)


@cli.command()
@click.option(
    "--network-file",
    "-n",
    type=click.Path(exists=True),
    default=CACHE_PATH / "tap_network_dataframe.parquet",
)
@click.option(
    "--od-file", "-d", type=click.Path(exists=True), default=CACHE_PATH / "tap_highway_tons.parquet"
)
@click.option("--max-gap", "-g", type=float, default=1e-8)
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
def dagster():
    logger.info("Running dagster webserver.")
    subprocess.run("dagster dev")


if __name__ == "__main__":
    cli()
