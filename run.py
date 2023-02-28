import logging

from src.util.log import configure_logging

configure_logging(output_file=True)


def main():
    logger = logging.getLogger(__name__)
    logger.info("hi")


if __name__ == "__main__":
    main()
