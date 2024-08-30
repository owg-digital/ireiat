import dagster
from ireiat.data_pipeline.io_manager import asset_spec_factory

marine_links_spec = dagster.AssetSpec(
    key=dagster.AssetKey("marine_network_links_spec"),
    description="Publicly available US Army Corps of Engineers data for marine network links",
    metadata={
        "format": "zip",
        "filename": "marine_links.zip",
        "source_path": "raw/",
        "dashboard_url": dagster.MetadataValue.url(
            "https://usace.contentdm.oclc.org/digital/api/collection/p16021coll2/id/3010/download"
        ),
    },
)
marine_links_src = asset_spec_factory(marine_links_spec)
