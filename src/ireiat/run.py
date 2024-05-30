import logging
import os
import subprocess

import click

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
def solve():
    logger.warning("Not yet implemented.")


@cli.command()
def dagster():
    logger.info("Running dagster webserver.")
    subprocess.run("dagster dev")


if __name__ == "__main__":
    cli()
