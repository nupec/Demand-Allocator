from fastapi import APIRouter, UploadFile, Query, HTTPException
from typing import Optional
from fastapi.responses import JSONResponse
import pandas as pd

from app.methods.geodesic_method import geodesic_method
from app.methods.real_distance_pandana_method import real_distance_pandana_method
from app.preprocessing.common import prepare_data

router = APIRouter()

@router.post("/geodesic_method/")
def test_geodesic_method(
    opportunities_file: UploadFile,
    demands_file: UploadFile,
    state: Optional[str] = Query(None, description="State (optional)"),
    city: Optional[str] = Query(None, description="City (optional)")
):
    # 1) Prepare data using the "prepare_data" function
    error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city = prepare_data(
        opportunities_file, demands_file, state, city
    )

    if error:
        return error

    # 2) Execute geodesic method for allocation
    result_df = geodesic_method(demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city)

    # 3) Return the result as JSON
    return JSONResponse(content=result_df.to_dict(orient="records"))


@router.post("/real_distance_method/")
def test_real_distance_method(
    opportunities_file: UploadFile,
    demands_file: UploadFile,
    state: Optional[str] = Query(None, description="State (optional)"),
    city: Optional[str] = Query(None, description="City (optional)"),
    num_threads: int = Query(1, description="Number of threads")
):
    # 1) Prepare data using the "prepare_data" function
    error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city = prepare_data(
        opportunities_file, demands_file, state, city
    )

    if error:
        return error

    # 2) Execute real distance allocation method using the pandana approach
    result_df = real_distance_pandana_method(
        demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city, city_name=city, num_threads=num_threads
    )

    # 3) Return the result as JSON
    return JSONResponse(content=result_df.to_dict(orient="records"))
