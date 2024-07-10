import dagster

from ireiat.data_pipeline.metadata import observation_function


narn_links_src = dagster.SourceAsset(
    key=dagster.AssetKey("narn_rail_network_links"),
    observe_fn=observation_function,
    description="Publicly available GIS data for railway network links",
    io_manager_key="custom_io_manager",
    metadata={
        "format": "zip",
        "filename": "narn_rail_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://stg-arcgisazurecdataprod.az.arcgis.com/exportfiles-273-8581/NTAD_North_American_Rail_Network_Lines_4887242439196784421.zip?sv=2018-03-28&sr=b&sig=D2nErdxtbtfHwa7pcy9dTe6iuiZ46IsKIp7Z%2FisnzGs%3D&se=2024-07-03T14%3A07%3A01Z&sp=r"
        ),
    },
)

intermodal_terminals_src = dagster.SourceAsset(
    key=dagster.AssetKey("intermodal_terminals"),
    observe_fn=observation_function,
    description="CSV containing intermodal terminal information, including mapping to corresponding rail network nodes.",
    io_manager_key="custom_io_manager",
    metadata={
        "format": "csv",
        "filename": "im_terminals.csv",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://raw.githubusercontent.com/owg-digital/ireiat/hd-rail-network/data/im_terminals.csv"
        ),
    },
)

