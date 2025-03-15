import json
import uuid
import os
import pandas as pd
import unicodedata
import logging

from fastapi import APIRouter, UploadFile, File, Form, Query, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import List
from .knn_route import MethodEnum, OutputFormatEnum
from app.preprocessing.common import prepare_data
from app.methods.knn_model import allocate_demands_knn


logger = logging.getLogger(__name__)

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

    logger.info("Request to allocate demands by city list. state=%s, k=%d, method=%s, format=%s", state, k, method, output_format)
    logger.debug("Raw city_payload_str: %s", city_payload_str)

    # Convert the JSON string to a dictionary and validate it with Pydantic
    try:
        payload_dict = json.loads(city_payload_str)  # convert string -> dict
        city_payload = CityAllocationRequest(**payload_dict)
    except json.JSONDecodeError as e:
        logger.exception("JSON decode error for city_payload_str.")
        raise HTTPException(status_code=400, detail="Invalid JSON in field city_payload_str.")

    logger.info("Cities received: %s", city_payload.cities)
    
    logger.info("Calling prepare_data to read input files (without city filter).")
    # Prepare DataFrames (without filtering by city now, as it will be done manually)
    error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city = prepare_data(
        opportunities_file, demands_file, state=state, city=None
    )
    if error:
        logger.error("Error in prepare_data: %s", error)
        return error

    # Check if the city list is empty
    if not city_payload.cities:
        logger.warning("No cities were provided in the payload.")
        raise HTTPException(status_code=400, detail="No city was provided.")

    list_of_dataframes = []

    # Loop through each requested city
    for city in city_payload.cities:
        # Normalize city name for comparison
        city_norm = normalize_str(city)
        logger.info("Processing city: '%s' (normalized='%s')", city, city_norm)
        
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
                logger.warning("No matching records found for city '%s'. Skipping.", city)
                continue

            logger.info("Allocating demands for city '%s' with k=%d, method=%s", city, k, method)    
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
            logger.exception("ValueError in allocate_demands_knn for city '%s'.", city)
            raise HTTPException(status_code=400, detail=str(ve))

    # If no results were obtained for any city, return 404
    if not list_of_dataframes:
        logger.error("No results returned for any of the requested cities.")
        raise HTTPException(
            status_code=404,
            detail="No demand/opportunity returned results for the provided cities."
        )

    # Concatenate all resulting DataFrames
    final_result_df = pd.concat(list_of_dataframes, ignore_index=True)
    logger.info("Concatenated final DataFrame shape: %s", final_result_df.shape)

    # Return the result in the requested format (csv, geojson, or json)
    file_id = str(uuid.uuid4())
    OUTPUT_DIR = "/tmp/api_output/"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if output_format == "csv":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.csv")
        final_result_df.to_csv(output_file, index=False)
        logger.info("Returning CSV file: %s", output_file)
        return FileResponse(output_file, media_type="text/csv", filename=os.path.basename(output_file))

    elif output_format == "geojson":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.geojson")
        final_result_df.to_json(output_file, index=False, orient="records")
        logger.info("Returning GeoJSON file: %s", output_file)
        return FileResponse(output_file, media_type="application/geo+json", filename=os.path.basename(output_file))

    elif output_format == "json":
        logger.info("Returning JSON response directly.")
        return JSONResponse(content=final_result_df.to_dict(orient="records"))

    else:
        logger.error("Invalid output format: %s", output_format)
        raise HTTPException(status_code=400, detail="Invalid output format. Use 'csv', 'geojson', or 'json'.")