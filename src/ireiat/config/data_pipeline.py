from dagster import Config
from pydantic import Field


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
    rail_config: RailConfig
    highway_config: HighwayConfig
    marine_config: MarineConfig
