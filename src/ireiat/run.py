import logging

import click

from ireiat.config import Config
from ireiat.util.http import download_files
from ireiat.util.logging_ import configure_logging

configure_logging(output_file=True)
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--force/--no-force", default=False, help="Force a re-download of needed files"
)
def execute_all(force):
    config = Config.from_yaml()
    paths, urls = config.download_targets
    logger.info("Getting raw files...")
    download_files(urls, paths, force=force)


if __name__ == "__main__":
    execute_all()
