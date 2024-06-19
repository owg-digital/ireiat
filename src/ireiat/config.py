import os
from pathlib import Path

from ireiat.util.faf_constants import FAFCommodity

# cache-related parameters
CACHE_PATH = Path(os.getenv("HOMEPATH", "~")) / ".ireiat"
INTERMEDIATE_PATH = "intermediate"
INTERMEDIATE_DIRECTORY_ARGS = {"source_path": INTERMEDIATE_PATH}

# GIS-related parameters
RADIUS_EARTH_MILES = 3958.8
LATLONG_CRS = "EPSG:4326"
ALBERS_CRS = "EPSG:5070"


# FAF-related parameters
SUM_TONS_TOLERANCE = 1e-5
FAF_TONS_TARGET_FIELD = "tons_2022"
NON_CONTAINERIZABLE_COMMODITIES = [
    FAFCommodity.LIVE_ANIMALS_FISH,
    FAFCommodity.OTHER_AG_PRODS,
    FAFCommodity.MEAT_SEAFOOD,
    FAFCommodity.CRUDE_PETROLEUM,
    FAFCommodity.COAL,
    FAFCommodity.GRAVEL,
    FAFCommodity.GASOLINE,
    FAFCommodity.NATURAL_GAS_AND_OTHER_FOSSIL_PRODUCTS,
    FAFCommodity.FURNITURE,
    FAFCommodity.PHARMACEUTICALS,
    FAFCommodity.MIXED_FREIGHT,
]

# exclude some states, regions from the input data
EXCLUDED_FIPS_CODES_MAP = {
    "Alaska": "02",
    "Hawaii": "15",
    "Puerto Rico": "72",
    "American Samoa": "60",
    "Northern Mariana Islands": "69",
    "Guam": "66",
    "US Virgin Islands": "78",
}


# TAP-related parameters
HIGHWAY_CAPACITY_TONS = (
    80000  # TODO (NP): Placeholder while we translate from tons->vehicles or put a tonnage limit
)
HIGHWAY_BETA = 4  # the exponent in the congestion term
HIGHWAY_ALPHA = 0.15  # the scalar in the congestion term
