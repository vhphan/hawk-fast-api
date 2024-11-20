from enum import Enum
from typing import Callable, Any

MNOs = [
    'Maxis',
    'Umobile',
    'YTL',
    'Digi',
    'Celcom',
    'TM',
]


class GroupBy(Enum):
    REGION = 'region'
    CLUSTER = 'cluster'
    CELLS = 'cells'

TransformFunction = Callable[[int, int, list[dict[str, Any]], GroupBy], dict[str, Any]]

DAILY_MAX_POINTS = 31
