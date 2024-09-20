import dagster

from ireiat.data_pipeline.io_manager import asset_spec_factory

narn_links_spec = dagster.AssetSpec(
    key=dagster.AssetKey("narn_rail_network_links_spec"),
    description="Publicly available GIS data for railway network links",
    metadata={
        "format": "zip",
        "filename": "narn_rail_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://hub.arcgis.com/api/download/v1/items/e143f436d4774402aa8cca1e663b1d24/shapefile?redirect=true&layers=0"
        ),
    },
)
narn_links_src = asset_spec_factory(narn_links_spec)

intermodal_terminals_spec = dagster.AssetSpec(
    key=dagster.AssetKey("intermodal_terminals_spec"),
    description="CSV containing intermodal terminal information, including mapping to corresponding rail network nodes.",
    metadata={
        "format": "csv",
        "filename": "im_terminals.csv",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://raw.githubusercontent.com/owg-digital/ireiat/master/data/rail_network/im_terminals.csv"
        ),
    },
)
intermodal_terminals_src = asset_spec_factory(intermodal_terminals_spec)

rail_interchange_impedances_spec = dagster.AssetSpec(
    key=dagster.AssetKey("rail_interchange_impedances_spec"),
    description="YAML file containing rail interchange impedances",
    metadata={
        "format": "yaml",
        "filename": "rail_interchange_impedances.yaml",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            # "https://raw.githubusercontent.com/owg-digital/ireiat/master/data/rail_network/rail_interchange_impedances.yaml"
            "https://raw.githubusercontent.com/owg-digital/ireiat/msm-interchange-impedance/data/rail_network/rail_interchange_impedances.yaml"
        ),
    },
)
rail_interchange_impedances_src = asset_spec_factory(rail_interchange_impedances_spec)
