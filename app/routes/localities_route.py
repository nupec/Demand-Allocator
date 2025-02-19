from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from enum import Enum
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import os
import uuid
from unidecode import unidecode
import json

from app.preprocessing.common import prepare_data
from app.methods.knn_model import allocate_demands_knn
from app.methods.real_distance_pandana_method import real_distance_pandana_method


class MethodEnum(str, Enum):
    pandana_real_distance = "pandana_real_distance"
    geodesic = "geodesic"

class OutputFormatEnum(str, Enum):
    csv = "csv"
    geojson = "geojson"
    json = "json"

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
async def allocate_demands_for_localities(
    opportunities_file: UploadFile = File(...),
    demands_file: UploadFile = File(...),
    payload: str = Form(...)
):
    try:
        # Convert JSON string received via Form into a dictionary
        payload_data = json.loads(payload)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format in payload.")

    # Create a Pydantic object with the extracted data
    try:
        payload_obj = LocalitiesInput(**payload_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing payload: {str(e)}")

    # 1) Read and prepare data using the "prepare_data" function
    error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city = prepare_data(
        opportunities_file,
        demands_file,
        state=payload_obj.state,
        city=None  
    )

    if error:
        return error

    # 2) Filter localities
    if not col_city:
        raise HTTPException(
            status_code=400,
            detail="Unable to infer the city column. Please check the opportunities data."
        )

    localities_normalized = [unidecode(loc).lower().strip() for loc in payload_obj.localities]

    if "NM_MUN" in demands_gdf.columns:
        demands_gdf = demands_gdf[
            demands_gdf["NM_MUN"].apply(lambda x: unidecode(str(x)).lower().strip() in localities_normalized)
        ]

    opportunities_gdf = opportunities_gdf[
        opportunities_gdf[col_city].apply(lambda x: unidecode(str(x)).lower().strip() in localities_normalized)
    ]

    if demands_gdf.empty or opportunities_gdf.empty:
        raise HTTPException(
            status_code=404,
            detail="No demand or opportunity found for the provided localities."
        )

    # 3) Choose allocation method
    if payload_obj.method == MethodEnum.pandana_real_distance:
        result_df = real_distance_pandana_method(
            demands_gdf,
            opportunities_gdf,
            col_demand_id,
            col_name,
            col_city,
            city_name=None,
            num_threads=1
        )
    else:
        result_df = allocate_demands_knn(
            demands_gdf,
            opportunities_gdf,
            col_demand_id,
            col_name,
            col_city,
            k=payload_obj.k,
            method="geodesic",
            city_name=None
        )

    # 4) Return response in the chosen format
    file_id = str(uuid.uuid4())
    if payload_obj.output_format == OutputFormatEnum.csv:
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.csv")
        result_df.to_csv(output_file, index=False)
        return FileResponse(output_file, media_type="text/csv", filename=os.path.basename(output_file))

    elif payload_obj.output_format == OutputFormatEnum.geojson:
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.geojson")
        result_df.to_json(output_file, orient="records")
        return FileResponse(output_file, media_type="application/geo+json", filename=os.path.basename(output_file))

    return JSONResponse(content=result_df.to_dict(orient="records"))
