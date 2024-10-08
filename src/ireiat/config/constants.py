import os
from pathlib import Path

# cache-related parameters
CACHE_PATH = Path(os.getenv("HOMEPATH", "~")) / ".ireiat"
INTERMEDIATE_PATH = "intermediate"
RAW_PATH = "raw"
INTERMEDIATE_DIRECTORY_ARGS = {"source_path": INTERMEDIATE_PATH}

# GIS-related parameters
RADIUS_EARTH_MILES = 3958.8
METERS_PER_MILE = 1609.34
LATLONG_CRS = "EPSG:4326"
ALBERS_CRS = "EPSG:5070"

# FAF-related parameters
SUM_TONS_TOLERANCE = 1e-5

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

# Rail
# standardize railway codes both in the NARN data and on intermodal terminals
RR_MAPPING = {
    "CP": "CPKC",
    "CSX": "CSXT",
    "CPRS": "CPKC",
    "KCS": "CPKC",
    "KCSM": "CPKC",
    "JXPT": "JXP",
}
