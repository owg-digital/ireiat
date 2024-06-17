from enum import IntEnum


class FAFMode(IntEnum):
    """See https://www.bts.gov/sites/bts.dot.gov/files/2021-02/FAF5-User-Guide.pdf"""

    TRUCK = 1
    RAIL = 2
    WATER = 3
    AIR = 4
    MULTIPLE_AND_MAIL = 5
    PIPELINE = 6
    OTHER_AND_UNKNOWN = 7
    NO_DOMESTIC_MODE = 8
