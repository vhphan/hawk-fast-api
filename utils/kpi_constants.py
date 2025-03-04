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
    NO_GROUP = 'no_group'
    BAND = 'band'


TransformFunction = Callable[[int, int, list[dict[str, Any]], GroupBy], dict[str, Any]]

DAILY_MAX_POINTS = 60
HOURLY_MAX_POINTS = 14 * 24
BANDS = ['N3', 'N7']
