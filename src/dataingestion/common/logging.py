import logging


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("dataingestion")


def get_logger(name: str = "dataingestion") -> logging.Logger:
    return logging.getLogger(name)