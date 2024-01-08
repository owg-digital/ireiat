import logging

import click

from ireiat.config import Config

from ireiat.util.http import download_files
from ireiat.util.logging_ import configure_logging

configure_logging(output_file=True)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--debug/--no-debug", default=False)
def cli(debug):
    logger.info(f"Debug mode is {'on' if debug else 'off'}")


@cli.command()
@click.option(
    "--force/--no-force", default=False, help="Force a re-download of needed files"
)
def download(force):
    config = Config.from_yaml()
    paths, urls = config.download_targets
    logger.info("Getting raw files...")
    download_files(urls, paths, force=force)


@cli.command()
def naive():
    logger.info("Running stuff.")


if __name__ == "__main__":
    cli()
