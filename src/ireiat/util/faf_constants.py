from enum import IntEnum, StrEnum


class FAFMode(IntEnum):
    """FAF transit mode. See https://www.bts.gov/sites/bts.dot.gov/files/2021-02/FAF5-User-Guide.pdf"""

    TRUCK = 1
    RAIL = 2
    WATER = 3
    AIR = 4
    MULTIPLE_AND_MAIL = 5
    PIPELINE = 6
    OTHER_AND_UNKNOWN = 7
    NO_DOMESTIC_MODE = 8


class FAFCommodity(StrEnum):
    """FAF transit mode. See https://www.bts.gov/sites/bts.dot.gov/files/2021-02/FAF5-User-Guide.pdf"""

    LIVE_ANIMALS_FISH = "01"
    CEREAL_GRAINS = "02"
    OTHER_AG_PRODS = "03"
    ANIMAL_FEED = "04"
    MEAT_SEAFOOD = "05"
    MILLED_GRAIN_PRODS = "06"
    OTHER_FOODSTUFFS = "07"
    ALCOHOLIC_BEVERAGES = "08"
    TOBACCO_PRODS = "09"
    BUILDING_STONE = "10"
    NATURAL_SANDS = "11"
    GRAVEL = "12"
    NONMETALLIC_MINERALS = "13"
    METALLIC_ORES = "14"
    COAL = "15"
    CRUDE_PETROLEUM = "16"
    GASOLINE = "17"
    FUEL_OILS = "18"
    NATURAL_GAS_AND_OTHER_FOSSIL_PRODUCTS = "19"
    BASIC_CHEMICALS = "20"
    PHARMACEUTICALS = "21"
    FERTILIZERS = "22"
    CHEMICAL_PRODS = "23"
    PLASTICS_RUBBER = "24"
    LOGS = "25"
    WOOD_PRODS = "26"
    NEWSPRINT_PAPER = "27"
    PAPER_ARTICLES = "28"
    PRINTED_PRODS = "29"
    TEXTILES_LEATHER = "30"
    NONMETAL_MIN_PRODS = "31"
    BASE_METALS = "32"
    ARTICLESBASE_METAL = "33"
    MACHINERY = "34"
    ELECTRONICS = "35"
    MOTORIZED_VEHICLES = "36"
    TRANSPORT_EQUIP = "37"
    PRECISION_INSTRUMENTS = "38"
    FURNITURE = "39"
    MISC_MFG_PRODS = "40"
    WASTE_SCRAP = "41"
    MIXED_FREIGHT = "43"
