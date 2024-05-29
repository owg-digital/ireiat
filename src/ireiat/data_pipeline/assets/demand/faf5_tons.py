import dagster
import pandas as pd

from ireiat.data_pipeline.metadata import publish_metadata


@dagster.asset(io_manager_key="custom_io_manager", metadata={"format": "parquet"})
def faf5_truck_demand(
    context: dagster.AssetExecutionContext, faf5_demand_src: pd.DataFrame
) -> pd.DataFrame:
    """FAF5 demand by truck"""
    TARGET_FIELD = "tons_2022"

    is_by_truck = faf5_demand_src["dms_mode"] == 1  # by truck
    faf_truck_pdf = faf5_demand_src.loc[is_by_truck]
    total_road_tons_od_pdf = faf_truck_pdf.groupby(["dms_orig", "dms_dest"], as_index=False)[
        [TARGET_FIELD]
    ].sum()
    publish_metadata(context, total_road_tons_od_pdf)
    return total_road_tons_od_pdf
