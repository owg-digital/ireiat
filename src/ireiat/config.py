import os
from pathlib import Path

from ireiat.util.faf_constants import FAFCommodity

# cache-related parameters
CACHE_PATH = Path(os.getenv("HOMEPATH", "~")) / ".ireiat"
INTERMEDIATE_PATH = "intermediate"
INTERMEDIATE_DIRECTORY_ARGS = {"source_path": INTERMEDIATE_PATH}

# GIS-related parameters
RADIUS_EARTH_MILES = 3958.8
METERS_PER_MILE = 1609.34
LATLONG_CRS = "EPSG:4326"
ALBERS_CRS = "EPSG:5070"

# FAF-related parameters
SUM_TONS_TOLERANCE = 1e-5
FAF_TONS_TARGET_FIELD = "tons_2022"

#: List of commodities that are not containerizable
NON_CONTAINERIZABLE_COMMODITIES = [
    FAFCommodity.LIVE_ANIMALS_FISH,
    FAFCommodity.CEREAL_GRAINS,
    FAFCommodity.OTHER_AG_PRODS,
    FAFCommodity.ANIMAL_FEED,
    FAFCommodity.BUILDING_STONE,
    FAFCommodity.NATURAL_SANDS,
    FAFCommodity.GRAVEL,
    FAFCommodity.NONMETALLIC_MINERALS,
    FAFCommodity.METALLIC_ORES,
    FAFCommodity.COAL,
    FAFCommodity.CRUDE_PETROLEUM,
    FAFCommodity.GASOLINE,
    FAFCommodity.FUEL_OILS,
    FAFCommodity.NATURAL_GAS_AND_OTHER_FOSSIL_PRODUCTS,
    FAFCommodity.BASIC_CHEMICALS,
    FAFCommodity.FERTILIZERS,
    FAFCommodity.LOGS,
    FAFCommodity.WOOD_PRODS,
    FAFCommodity.NEWSPRINT_PAPER,
    FAFCommodity.NONMETAL_MIN_PRODS,
    FAFCommodity.BASE_METALS,
    FAFCommodity.WASTE_SCRAP,
]

#: Exclude some states, regions from the input data
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
# Highway
HIGHWAY_CAPACITY_TONS = (
    80000  # TODO (NP): Placeholder while we translate from tons->vehicles or put a tonnage limit
)
HIGHWAY_BETA = 4  # the exponent in the congestion term
HIGHWAY_ALPHA = 0.15  # the scalar in the congestion term
HIGHWAY_DEFAULT_MPH_SPEED = (
    50  # used in the rail network for county centroid connections to IM facilities
)

# Rail
RAIL_DEFAULT_TERMINAL_CAPACITY_TONS = 100000
RAIL_DEFAULT_LINK_CAPACITY_TONS = 60000
RAIL_DEFAULT_MPH_SPEED = 20  # miles per hour
# Beta - the exponent in the congestion term
RAIL_BETA_IM = 1
RAIL_BETA = 3.5
# Alpha - # the scalar in the congestion term
RAIL_ALPHA_IM = 1
RAIL_ALPHA = 0.1
# standardize railway codes both in the NARN data and on intermodal terminals
RR_MAPPING = {
    "CP": "CPKC",
    "CSX": "CSXT",
    "CPRS": "CPKC",
    "KCS": "CPKC",
    "KCSM": "CPKC",
    "JXPT": "JXP",
}
IM_CAPACITY_TONS = 500000 * 15  # assume 500K containers per year at 15 net tons per container
IM_SEARCH_RADIUS_MILES = 250  # radius to search around each county centroid

# Marine
MARINE_CAPACITY_TONS = 150000
MARINE_BETA = 1  # the exponent in the congestion term
MARINE_ALPHA = 0.001  # the scalar in the congestion term
MARINE_DEFAULT_MPH_SPEED = 15  # miles per hour
