import sys
import logging
from collections.abc import Callable
from typing import NoReturn

logger = logging.getLogger(__name__)


def assert_that(callable: Callable[[], bool], message: str | None = None) -> None:
    if not callable():
        die(message)


def die(message: str | None) -> NoReturn:
    if message:
        logger.error(message)

    sys.exit(1)
