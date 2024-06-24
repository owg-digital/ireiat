import dagster

from ireiat.data_pipeline.metadata import observation_function


marine_links_src = dagster.SourceAsset(
    key=dagster.AssetKey("marine_network_links"),
    observe_fn=observation_function,
    description="Publicly available US Army Corps of Engineers data for marine network links",
    io_manager_key="custom_io_manager",
    metadata={
        "format": "zip",
        "filename": "marine_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://usace.contentdm.oclc.org/digital/api/collection/p16021coll2/id/3010/download"
        ),
    },
)
