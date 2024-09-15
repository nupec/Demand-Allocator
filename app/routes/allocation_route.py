from fastapi import APIRouter, UploadFile, Query
from app.allocation.allocation import allocate_demands
from app.allocation.common import prepare_data
from typing import Optional

router = APIRouter()

@router.post("/allocate_demands/")
def allocate_demands_api(
  establishments_file: UploadFile,
  demands_file: UploadFile,
  state: Optional[str] = Query(None, description="State (optional, if not provided it will be allocated to the entire region)"),
  city: Optional[str] = None
):
    error, demands_gdf, establishments_gdf, col_demand_id, col_name, col_city = prepare_data(establishments_file, demands_file, state, city)

    if error:
        return error

    result_df = allocate_demands(demands_gdf, establishments_gdf, col_demand_id, col_name, col_city)

    return result_df.to_dict(orient='records')
