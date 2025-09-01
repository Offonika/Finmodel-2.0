from __future__ import annotations

from threading import Timer
from typing import Any, Callable

from finmodel.logger import get_logger

logger = get_logger(__name__)


def schedule_after_meal(callback: Callable[[], Any], minutes_after: float) -> Timer | None:
    """Schedule a callback to run after a given number of minutes.

    The function validates that ``minutes_after`` is a positive number before scheduling.
    If validation fails, an error is logged and the function returns ``None``.

    Parameters
    ----------
    callback: Callable
        A function to execute after the delay.
    minutes_after: float
        Number of minutes to wait before executing ``callback``.

    Returns
    -------
    Timer | None
        The scheduled :class:`threading.Timer` instance or ``None`` if validation fails.
    """
    if not isinstance(minutes_after, (int, float)) or minutes_after <= 0:
        logger.error("minutes_after must be a positive number, got %r", minutes_after)
        return None

    timer = Timer(minutes_after * 60, callback)
    timer.start()
    return timer
