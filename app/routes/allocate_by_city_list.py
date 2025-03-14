import json
import uuid
import os
import pandas as pd
import unicodedata

from fastapi import APIRouter, UploadFile, File, Form, Query, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import List
from .knn_route import MethodEnum, OutputFormatEnum
from app.preprocessing.common import prepare_data
from app.methods.knn_model import allocate_demands_knn


# Payload model for cities
class CityAllocationRequest(BaseModel):
    cities: List[str]

# Function to normalize strings (remove accents, spaces, and convert to lowercase)
def normalize_str(s: str) -> str:
    return unicodedata.normalize("NFKD", s.strip().lower()).encode("ascii", "ignore").decode("utf-8")

router = APIRouter()

@router.post("/allocate_demands_by_city_list/")
def allocate_demands_by_city_list(
    # JSON city_payload as a string (to bypass multipart limitation)
    city_payload_str: str = Form(...),

    opportunities_file: UploadFile = File(...),
    demands_file: UploadFile = File(...),
    state: str = Query("", description="State (optional)"),
    k: int = Query(1, description="Number of neighbors for KNN"),
    method: MethodEnum = Query(MethodEnum.pandana_real_distance, description="Allocation method"),
    output_format: OutputFormatEnum = Query(OutputFormatEnum.csv, description="Output format: 'csv', 'geojson', or 'json'")
):
    """
    Endpoint that receives (multipart/form-data):
      - city_payload_str: JSON string (e.g., {"cities":["Itacoatiara","Maués"]})
      - opportunities_file: geojson file
      - demands_file: geojson file
      - query params: state, k, method, output_format

    And returns the resulting allocation (in CSV, JSON, or GEOJSON).
    """

    # Convert the JSON string to a dictionary and validate it with Pydantic
    try:
        payload_dict = json.loads(city_payload_str)  # convert string -> dict
        city_payload = CityAllocationRequest(**payload_dict)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in field city_payload_str.")

    # Prepare DataFrames (without filtering by city now, as it will be done manually)
    error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city = prepare_data(
        opportunities_file, demands_file, state=state, city=None
    )
    if error:
        return error

    # Check if the city list is empty
    if not city_payload.cities:
        raise HTTPException(status_code=400, detail="No city was provided.")

    list_of_dataframes = []

    # Loop through each requested city
    for city in city_payload.cities:
        # Normalize city name for comparison
        city_norm = normalize_str(city)
        
        try:
            # Filter the opportunities GeoDataFrame using the same normalization
            opp_city = opportunities_gdf[
                opportunities_gdf[col_city].astype(str).apply(normalize_str) == city_norm
            ]
            # Filter the demands GeoDataFrame using the same normalization in column "NM_MUN"
            demands_city = demands_gdf[
                demands_gdf["NM_MUN"].astype(str).apply(normalize_str) == city_norm
            ]
            # If no records are found for the city, skip to the next one
            if opp_city.empty or demands_city.empty:
                continue

            # Allocate using the KNN function
            result_df = allocate_demands_knn(
                demands_city,
                opp_city,
                col_demand_id,
                col_name,
                col_city,
                k=k,
                method=method,
                city_name=city,  # Required for the pandana method if caching is desired
                num_threads=1
            )
            # Mark the city in each row of the result
            result_df["city_allocated"] = city
            list_of_dataframes.append(result_df)

        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))

    # If no results were obtained for any city, return 404
    if not list_of_dataframes:
        raise HTTPException(
            status_code=404,
            detail="No demand/opportunity returned results for the provided cities."
        )

    # Concatenate all resulting DataFrames
    final_result_df = pd.concat(list_of_dataframes, ignore_index=True)

    # Return the result in the requested format (csv, geojson, or json)
    file_id = str(uuid.uuid4())
    OUTPUT_DIR = "/tmp/api_output/"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if output_format == "csv":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.csv")
        final_result_df.to_csv(output_file, index=False)
        return FileResponse(output_file, media_type="text/csv", filename=os.path.basename(output_file))

    elif output_format == "geojson":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.geojson")
        final_result_df.to_json(output_file, index=False, orient="records")
        return FileResponse(output_file, media_type="application/geo+json", filename=os.path.basename(output_file))

    elif output_format == "json":
        return JSONResponse(content=final_result_df.to_dict(orient="records"))

    else:
        raise HTTPException(status_code=400, detail="Invalid output format. Use 'csv', 'geojson', or 'json'.")