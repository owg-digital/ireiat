import logging

import click

from src.config import Config
from src.util.http import download_files
from src.util.logging_ import configure_logging

configure_logging(output_file=True)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--force/--no-force", default=False, help="Force a re-download of needed files"
)
def download_raw_data(force):
    config = Config.from_yaml()
    paths, urls = config.download_targets
    logger.info("Getting raw files...")
    download_files(urls, paths, force=force)


if __name__ == "__main__":
    download_raw_data()
