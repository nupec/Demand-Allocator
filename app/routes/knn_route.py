from fastapi import APIRouter, UploadFile, Query, HTTPException, Body
from pydantic import BaseModel
from typing import Optional, List
from enum import Enum

from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import os
import uuid

from app.preprocessing.common import prepare_data
from app.methods.knn_model import allocate_demands_knn
from app.methods.real_distance_pandana_method import real_distance_pandana_method


# Enums that you already used for 'method' and 'output_format'
class MethodEnum(str, Enum):
    pandana_real_distance = "pandana_real_distance"
    geodesic = "geodesic"

class OutputFormatEnum(str, Enum):
    csv = "csv"
    geojson = "geojson"
    json = "json"


# Create a model for the request body
class LocalitiesInput(BaseModel):
    localities: List[str]
    state: Optional[str] = None
    k: int = 1
    method: MethodEnum = MethodEnum.pandana_real_distance
    output_format: OutputFormatEnum = OutputFormatEnum.json

router = APIRouter()

OUTPUT_DIR = "/tmp/api_output/"
os.makedirs(OUTPUT_DIR, exist_ok=True)


@router.post("/allocate_demands_localities/")
def allocate_demands_localities(
    opportunities_file: UploadFile,
    demands_file: UploadFile,
    payload: LocalitiesInput = Body(...)
):
    """
    Route to receive *a list of localities* and return allocations
    only for these specific localities.
    """

    # 1) Read and prepare data (same as your main route):
    error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city = prepare_data(
        opportunities_file, 
        demands_file,
        state=payload.state,
        city=None  # here we pass None to not filter by `city` separately
    )

    if error:
        return error  # if there was an error in the columns, return the error

    # 2) Filter the GeoDataFrames by the provided localities
    #    Adjust for the exact name of the column where the municipality is.
    #    In prepare_data, 'col_city' is inferred. If it comes as None, review.
    if not col_city:
        raise HTTPException(
            status_code=400,
            detail="Não foi possível inferir a coluna de cidade (col_city) nos dados."
        )

    # Normalize the localities (using unidecode, lower) for comparison
    localities_normalized = [loc.strip().lower() for loc in payload.localities]

    # Filtering the demands_gdf (in case it has something like 'NM_MUN' or similar)
    # But if the city column for demands is different, adjust here
    from unidecode import unidecode

    def normalize_str(s):
        return unidecode(s).lower().strip() if isinstance(s, str) else ""

    if "NM_MUN" in demands_gdf.columns:
        demands_gdf = demands_gdf[
            demands_gdf["NM_MUN"].apply(lambda x: normalize_str(x) in localities_normalized)
        ]

    # Filtering the opportunities_gdf using the inferred col_city
    opportunities_gdf = opportunities_gdf[
        opportunities_gdf[col_city].apply(lambda x: normalize_str(x) in localities_normalized)
    ]

    if demands_gdf.empty or opportunities_gdf.empty:
        raise HTTPException(
            status_code=404,
            detail="No demand or opportunity found for the locations provided"
        )

    # 3) Call the allocation method
    if payload.method == MethodEnum.pandana_real_distance:
        result_df = real_distance_pandana_method(
            demands_gdf, 
            opportunities_gdf, 
            col_demand_id, 
            col_name, 
            col_city,
            city_name=None,
            num_threads=1
        )
    elif payload.method == MethodEnum.geodesic:
        # "k" only makes sense if it's geodesic KNN? Depends on your workflow.
        # If it's normal allocation, we call allocate_demands_knn:
        result_df = allocate_demands_knn(
            demands_gdf, 
            opportunities_gdf, 
            col_demand_id, 
            col_name, 
            col_city,
            k=payload.k,
            method="geodesic",
            city_name=None
        )
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid method. Choose 'pandana_real_distance' or 'geodesic'."
        )

    # 4) Return in the chosen format
    file_id = str(uuid.uuid4())
    if payload.output_format == OutputFormatEnum.csv:
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.csv")
        result_df.to_csv(output_file, index=False)
        return FileResponse(output_file, media_type="text/csv", filename=os.path.basename(output_file))

    elif payload.output_format == OutputFormatEnum.geojson:
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.geojson")
        # Needs to convert to a GeoDataFrame if it is actually geographic.
        # Assuming that "result_df" does not have geometry. If it does, use .to_file(..., driver="GeoJSON")
        # Here, I use a normal "to_json" for demonstration:
        result_df.to_json(output_file, orient="records")
        return FileResponse(output_file, media_type="application/geo+json", filename=os.path.basename(output_file))

    elif payload.output_format == OutputFormatEnum.json:
        return JSONResponse(content=result_df.to_dict(orient="records"))
    else:
        raise HTTPException(status_code=400, detail="Formato de saída inválido.")
