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
            "https://stg-arcgisazurecdataprod.az.arcgis.com/exportfiles-273-8581/NTAD_North_American_Rail_Network_Lines_4887242439196784421.zip?sv=2018-03-28&sr=b&sig=jRmQ9mKX3nBDsW47G9ZD9JVNAO15qqtzsFQ%2B3LcZmuA%3D&se=2024-07-10T21%3A34%3A03Z&sp=r"
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
