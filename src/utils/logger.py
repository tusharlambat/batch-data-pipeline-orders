import logging
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
DEFAULT_LOG_FILE = LOG_DIR / "pipeline.log"


def _has_file_handler(logger):
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            try:
                if Path(handler.baseFilename) == DEFAULT_LOG_FILE:
                    return True
            except Exception:
                continue
    return False


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # Let Airflow task handlers collect these logs in the UI as well.
    logger.propagate = True

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if not _has_file_handler(logger):
        file_handler = logging.FileHandler(str(DEFAULT_LOG_FILE))
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    root_logger = logging.getLogger()
    if not root_logger.handlers and not any(
        isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler)
        for handler in logger.handlers
    ):
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(console_handler)

    return logger
