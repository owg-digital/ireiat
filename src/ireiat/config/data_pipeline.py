from typing import Optional

import yaml
from dagster import Config
from pydantic import Field

FAF5_DEFAULT_TONS_FIELD = "tons_2022"


class FAFCommodity(Config):
    """Single FAF Commodity"""

    name: str
    sctg2: str = Field(min_length=2, max_length=2)
    containerizable: bool = Field(default=False)
    percentage_containerizable: float = Field(default=0.0, le=1, ge=0)

    @classmethod
    def from_args(cls, name, sctg2, containerizable, pct):
        arg_dict = {
            "name": name,
            "sctg2": sctg2,
            "containerizable": containerizable,
            "percentage_containerizable": pct,
        }
        return cls(**arg_dict)


def default_faf_commodity_factory() -> list[FAFCommodity]:
    return [
        FAFCommodity.from_args("LIVE_ANIMALS_FISH", "01", False, 0.0),
        FAFCommodity.from_args("CEREAL_GRAINS", "02", False, 0.0),
        FAFCommodity.from_args("OTHER_AG_PRODS", "03", False, 0.0),
        FAFCommodity.from_args("ANIMAL_FEED", "04", False, 0.0),
        FAFCommodity.from_args("MEAT_SEAFOOD", "05", True, 0.159),
        FAFCommodity.from_args("MILLED_GRAIN_PRODS", "06", True, 0.018),
        FAFCommodity.from_args("OTHER_FOODSTUFFS", "07", True, 0.504),
        FAFCommodity.from_args("ALCOHOLIC_BEVERAGES", "08", True, 0.455),
        FAFCommodity.from_args("TOBACCO_PRODS", "09", True, 1.0),
        FAFCommodity.from_args("BUILDING_STONE", "10", False, 0.0),
        FAFCommodity.from_args("NATURAL_SANDS", "11", False, 0.0),
        FAFCommodity.from_args("GRAVEL", "12", False, 0.0),
        FAFCommodity.from_args("NONMETALLIC_MINERALS", "13", False, 0.0),
        FAFCommodity.from_args("METALLIC_ORES", "14", False, 0.0),
        FAFCommodity.from_args("COAL", "15", False, 0.0),
        FAFCommodity.from_args("CRUDE_PETROLEUM", "16", False, 0.0),
        FAFCommodity.from_args("GASOLINE", "17", False, 0.0),
        FAFCommodity.from_args("FUEL_OILS", "18", False, 0.0),
        FAFCommodity.from_args("NATURAL_GAS_AND_OTHER_FOSSIL_PRODUCTS", "19", False, 0.0),
        FAFCommodity.from_args("BASIC_CHEMICALS", "20", False, 0.0),
        FAFCommodity.from_args("PHARMACEUTICALS", "21", True, 0.578),
        FAFCommodity.from_args("FERTILIZERS", "22", False, 0.0),
        FAFCommodity.from_args("CHEMICAL_PRODS", "23", True, 0.016),
        FAFCommodity.from_args("PLASTICS_RUBBER", "24", True, 0.056),
        FAFCommodity.from_args("LOGS", "25", False, 0.0),
        FAFCommodity.from_args("WOOD_PRODS", "26", False, 0.0),
        FAFCommodity.from_args("NEWSPRINT_PAPER", "27", False, 0.0),
        FAFCommodity.from_args("PAPER_ARTICLES", "28", True, 0.998),
        FAFCommodity.from_args("PRINTED_PRODS", "29", True, 1.0),
        FAFCommodity.from_args("TEXTILES_LEATHER", "30", True, 0.02),
        FAFCommodity.from_args("NONMETAL_MIN_PRODS", "31", False, 0.0),
        FAFCommodity.from_args("BASE_METALS", "32", False, 0.0),
        FAFCommodity.from_args("ARTICLESBASE_METAL", "33", True, 0.21),
        FAFCommodity.from_args("MACHINERY", "34", True, 0.593),
        FAFCommodity.from_args("ELECTRONICS", "35", True, 0.56),
        FAFCommodity.from_args("MOTORIZED_VEHICLES", "36", True, 0.641),
        FAFCommodity.from_args("TRANSPORT_EQUIP", "37", True, 0.031),
        FAFCommodity.from_args("PRECISION_INSTRUMENTS", "38", True, 0.021),
        FAFCommodity.from_args("FURNITURE", "39", True, 0.05),
        FAFCommodity.from_args("MISC_MFG_PRODS", "40", True, 0.108),
        FAFCommodity.from_args("WASTE_SCRAP", "41", False, 0.0),
        FAFCommodity.from_args("MIXED_FREIGHT", "43", True, 0.988),
    ]


class FAF5MasterConfig(Config):
    """Parent class to allow us to inject the FAF demand field into child classes"""

    faf_demand_field: str = Field(
        default=FAF5_DEFAULT_TONS_FIELD,
        description="The field in FAF used to aggregate demand ( in tons)",
    )


class FAF5FilterConfig(FAF5MasterConfig):
    """Allocates whether FAF commodities are considered as part of the demand. See FAF SCTG2 codes.
    https://www.bts.gov/sites/bts.dot.gov/files/2021-02/FAF5-User-Guide.pdf"""

    faf_commodities: list[FAFCommodity] = Field(default_factory=default_faf_commodity_factory)


class FAF5DemandConfig(FAF5MasterConfig):
    """Used to group and allocate unknown modes to this mode"""

    unknown_mode_percent: float = Field(
        default=0.3,
        description="Percentage of mixed mode FAF demand to allocate to the given modal network",
    )


class FAF5CountyConfig(FAF5MasterConfig):
    """Excludes county->county flows below a threshold"""

    county_to_county_ktons_threshold: float = Field(
        default=0.1,
        description="Annual kton threshold to be included in county->county flows. Anything smaller is excluded.",
    )


def _generate_class_1_rr_codes() -> list:
    return ["CSX", "BNSF", "UP", "NS", "CN", "KCS"]


class RailToRailImpedanceOverride(Config):
    """A class to specify individual rail owner impedances, currently only used within the `GeographicImpedance`
    class"""

    from_owner: str
    to_owner: str
    impedance: int


class GeographicImpedance(Config):
    """An override for impedance values within a geographic area. All impedance junctions with nodes
    that fall within `radius_miles` of a given lat/long will use this object's class_1_to_class_1_impedance
    and this configuration's default impedance for anything else. If `overrides` is specified, the specific
    rail-to-rail impedances will be used"""

    jr260: str
    latitude: float
    longitude: float
    class_1_to_class_1_impedance: int
    default_impedance: int = Field(
        default=275, description="Default impedance in this geographic area"
    )
    radius_miles: int = 20
    overrides: list[RailToRailImpedanceOverride] = Field(default_factory=list)


def _generate_rail_to_rail_impedance():
    param_dict = dict(from_owner="CSXT", to_owner="CN", impedance=276)
    return RailToRailImpedanceOverride(**param_dict)


def _generate_geographic_impedances():
    param_dict = dict(
        jr260="BATON",
        latitude=30.484715,
        longitude=-91.18676,
        class_1_to_class_1_impedance=500,
        default_impedance=250,
        radius_miles=20,
        overrides=[_generate_rail_to_rail_impedance()],
    )
    return [GeographicImpedance(**param_dict)]


class RailImpedanceConfig(Config):
    """Used to specify impedance parameters on the rail graph in equivalent miles"""

    class_1_rr_codes: list = Field(default_factory=_generate_class_1_rr_codes)
    default_impedance: int = Field(
        default=275, description="Default impedance to use when no match is found"
    )
    class_1_to_class_1_impedance: int = Field(
        default=750, description="Default impedance to use when switching between Class 1 RRs"
    )
    geographic_overrides: list[GeographicImpedance] = Field(
        default_factory=_generate_geographic_impedances
    )


class TAPFilterTonsConfig(Config):
    """Used to specify an optional quantile threshold to filter county|county tons for the mode"""

    quantile_threshold: Optional[float] = Field(
        default=0.99,
        description="If specified, only includes a county|county highway tonnage above the quantile value",
    )


class TAPNetworkConfig(Config):
    """Used to specify default configuration parameters"""

    default_speed_mph: int = Field(
        default=20,
        description="Default speed to use for (non-dray) rail links in the rail TAP network",
    )
    default_capacity_ktons: int = Field(
        default=100_000,
        description="Default capacity (in kilotons) to use for network edges without a custom capacity",
    )
    default_network_alpha: float = Field(
        default=0.1,
        description="Scalar, alpha, in TAP congestion term (alpha * (flow/capacity) ^ beta)",
    )
    default_network_beta: float = Field(
        default=2.0,
        description="Exponent, beta, in TAP congestion term (alpha * (flow/capacity) ^ beta)",
    )


class TAPRailConfig(TAPNetworkConfig):
    """Adjustable configuration parameters for running the rail network"""

    dray_default_speed_mph: int = Field(
        default=50, description="Default speed for drayage legs (for road segments) in the rail TAP"
    )
    dray_penalty_factor: int = Field(
        default=100,
        description="Factor to increase FFT on drayage legs to avoid exclusive use of the road network for the "
        "rail TAP",
    )
    capacity_ktons_per_track: int = Field(
        default=1000,
        description="Per track capacity (in kilotons) used in the rail network for NARN edges",
    )
    capacity_ktons_default: int = Field(
        default=100_000,
        description="Default capacity (in kilotons) to use for non-IM capacity and non-rail network (i.e. dray, "
        "impedance, etc.)",
    )
    intermodal_search_radius_miles: int = Field(
        default=250,
        description="Distance to search from an origin/destination centroid to an intermodal facility",
    )
    intermodal_facility_capacity_ktons: int = Field(
        default=7500,
        description="Default capacity (in kiltons) to use for IM facility inlet/outlet edges",
    )
    intermodal_edge_alpha: float = Field(
        default=1,
        description="Scalar, alpha, in TAP congestion term (alpha * (flow/capacity) ^ beta) for IM capacity links",
    )
    intermodal_edge_beta: float = Field(
        default=1,
        description="Exponent, beta, in TAP congestion term (alpha * (flow/capacity) ^ beta) for IM capacity links",
    )


default_tap_marine_config = TAPNetworkConfig(
    **{
        "default_speed_mph": 15,
        "default_capacity_ktons": 60_000,
        "default_network_alpha": 0.1,
        "default_network_beta": 2,
    }
)

default_tap_highway_config = TAPNetworkConfig(
    **{
        "default_speed_mph": 50,
        "default_capacity_ktons": 100_000,
        "default_network_alpha": 0.15,
        "default_network_beta": 4,
    }
)

default_tap_rail_config = TAPRailConfig(
    **{
        "default_speed_mph": 20,
        "default_capacity_ktons": 100_000,
        "default_network_alpha": 0.1,
        "default_network_beta": 2.0,
    }
)


def default_asset_mapping() -> dict:
    return {
        "faf_filtered_grouped_tons": {"config": FAF5FilterConfig()},
        "faf5_truck_demand": {"config": FAF5DemandConfig(**{"unknown_mode_percent": 0.3})},
        "faf5_rail_demand": {"config": FAF5DemandConfig(**{"unknown_mode_percent": 0.5})},
        "faf5_water_demand": {"config": FAF5DemandConfig(**{"unknown_mode_percent": 0.2})},
        "county_to_county_highway_tons": {
            "config": FAF5CountyConfig(**{"county_to_county_ktons_threshold": 1})
        },
        "county_to_county_rail_tons": {"config": FAF5MasterConfig()},
        "county_to_county_marine_tons": {"config": FAF5MasterConfig()},
        "impedance_rail_graph": {"config": RailImpedanceConfig()},
        "tap_highway_tons": {"config": TAPFilterTonsConfig(**{"quantile_threshold": None})},
        "tap_rail_tons": {"config": TAPFilterTonsConfig(**{"quantile_threshold": 0.8})},
        "tap_marine_tons": {"config": TAPFilterTonsConfig(**{"quantile_threshold": 0.6})},
        "tap_highway_network_dataframe": {"config": default_tap_highway_config},
        "tap_rail_network_dataframe": {"config": default_tap_rail_config},
        "tap_marine_network_dataframe": {"config": default_tap_marine_config},
    }


class DataPipelineConfig(Config):
    """Follows Dagster's specific configuration requirements for asset-specific configs"""

    ops: dict = Field(
        default_factory=default_asset_mapping,
        description="List of assets and associated configuration",
    )


# only used to update the root config file from the IDE when doing local dev (providing an example file)
if __name__ == "__main__":
    with open("../../../data/config.yaml", "w") as fp:
        yaml.dump(DataPipelineConfig().dict(), fp)
