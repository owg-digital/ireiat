from dagster import Config
from pydantic import Field


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


class DemandConfig(Config):
    """Contains FAF filter configuration. See FAF SCTG2 codes.
    https://www.bts.gov/sites/bts.dot.gov/files/2021-02/FAF5-User-Guide.pdf
    """

    unknown_mode_percent_truck: float = Field(
        default=0.3,
        description="Percentage of mixed mode FAF demand to allocate to highway network",
    )
    unknown_mode_percent_rail: float = Field(
        default=0.4, description="Percentage of mixed mode FAF demand to allocate to rail network"
    )
    unknown_mode_percent_marine: float = Field(
        default=0.3, description="Percentage of mixed mode FAF demand to allocat to marine network"
    )
    faf_commodities: list[FAFCommodity] = Field(default_factory=default_faf_commodity_factory)


class RailConfig(Config):
    """Adjustable configuration parameters for running the rail network"""

    default_speed_mph: int = Field(
        default=20,
        description="Default speed to use for (non-dray) rail links in the rail TAP network",
    )
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
    rail_network_alpha: float = Field(
        default=0.1,
        description="Scalar, alpha, in TAP congestion term (alpha * (flow/capacity) ^ beta) for rail links",
    )
    rail_network_beta: float = Field(
        default=2.0,
        description="Exponent, beta, in TAP congestion term (alpha * (flow/capacity) ^ beta) for rail links",
    )
    intermodal_edge_alpha: float = Field(
        default=1,
        description="Scalar, alpha, in TAP congestion term (alpha * (flow/capacity) ^ beta) for IM capacity links",
    )
    intermodal_edge_beta: float = Field(
        default=1,
        description="Exponent, beta, in TAP congestion term (alpha * (flow/capacity) ^ beta) for IM capacity links",
    )


class HighwayConfig(Config):
    """Adjustable configuration parameters for running the highway network"""

    default_capacity_ktons: int = Field(
        default=70_000,
        description="Default capacity (in kilotons) for each edge of the highway network",
    )
    highway_edge_alpha: float = Field(
        default=0.15,
        description="Scalar, alpha, in TAP congestion term (alpha * (flow/capacity) ^ beta) for highway links",
    )
    highway_edge_beta: float = Field(
        default=4,
        description="Exponent, beta, in TAP congestion term (alpha * (flow/capacity) ^ beta) for highway links",
    )


class MarineConfig(Config):
    """Adjustable configuration parameters for running the marine network"""

    default_capacity_ktons: int = Field(
        default=150_000,
        description="Default capacity (in kilotons) for each edge of the marine network",
    )
    default_speed_mph: int = Field(
        default=15, description="Default transit speed (miles per hour) on the marine network"
    )
    marine_edge_alpha: float = Field(
        default=0.01,
        description="Scalar, alpha, in TAP congestion term (alpha * (flow/capacity) ^ beta) for marine links",
    )
    marine_edge_beta: float = Field(
        default=4,
        description="Exponent, beta, in TAP congestion term (alpha * (flow/capacity) ^ beta) for marine links",
    )


class DataPipelineConfig(Config):
    faf_demand_field: str = Field(
        default="tons_2022", description="The field in FAF used to aggregate demand ( in tons)"
    )
    rail_config: RailConfig = RailConfig()
    highway_config: HighwayConfig = HighwayConfig()
    marine_config: MarineConfig = MarineConfig()
    demand_config: DemandConfig = DemandConfig()
