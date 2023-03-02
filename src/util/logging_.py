import logging
from datetime import datetime
from pathlib import Path
from typing import Optional


def configure_logging(
    str_format: str = "{asctime} {levelname} ({module}:{lineno:d}) {message}",
    output_file: bool = False,
    output_file_name: Optional[str] = None,
    output_file_dir: Path = Path("logs"),
    level=logging.INFO,
) -> None:
    """
    Configures logging called *once* at the application entry.

    :param str_format: logger string format
    :param output_file: bool, whether to log to an output file in output file directory
    :param output_file_name: optional, log file name (including extension)
        default YMDHms.log
    :param output_file_dir: optional, log output directory
    :param level: optional, logging level
    :return: None
    """
    # get the root logger and set its level
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # set logging format
    formatter = logging.Formatter(str_format, style="{", datefmt="%Y-%m-%d %H:%M:%S")

    # add a stream handler
    chandler = logging.StreamHandler()
    chandler.setFormatter(formatter)
    chandler.setLevel(level)
    root_logger.addHandler(chandler)

    # add a file handler if output_file is specified
    if output_file:
        default_log_key = datetime.now().strftime("%Y%m%d%H%M%S")
        default_log_fname = f"{default_log_key}.log"
        fname = output_file_name if output_file_name else default_log_fname
        fhandler = logging.FileHandler(output_file_dir / fname)
        fhandler.setFormatter(formatter)
        fhandler.setLevel(level)
        root_logger.addHandler(fhandler)
