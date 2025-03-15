from fastapi import APIRouter, UploadFile, Query, HTTPException
from enum import Enum
from typing import Optional
from fastapi.responses import FileResponse, JSONResponse
import logging
import os
import uuid
import pandas as pd
from app.preprocessing.common import prepare_data

from app.methods.knn_model import allocate_demands_knn

logger = logging.getLogger(__name__)
router = APIRouter()

# Step 1: Create the Enum for methods
class MethodEnum(str, Enum):
    pandana_real_distance = "pandana_real_distance"
    geodesic = "geodesic"

# Optional: Create a dropdown for output_format
class OutputFormatEnum(str, Enum):
    csv = "csv"
    geojson = "geojson"
    json = "json"

@router.post("/allocate_demands_knn/")
def allocate_demands_knn_api(
    opportunities_file: UploadFile,
    demands_file: UploadFile,
    state: str = Query("", description="State (optional)"),
    city: str = Query("", description="City (optional)"),
    k: int = Query(1, description="Number of neighbors for KNN"),
    # Step 2: Use the Enum for method selection
    method: MethodEnum = Query(MethodEnum.pandana_real_distance, description="Choose the allocation method"),
    output_format: OutputFormatEnum = Query(OutputFormatEnum.csv, description="Choose the output format: 'csv', 'geojson', or 'json'")
):
    logger.info("Received request to allocate demands using KNN.")
    logger.info("Parameters: state=%s, city=%s, k=%d, method=%s, output_format=%s", state, city, k, method, output_format)

    # Step 3: Prepare the GeoDataFrames and related columns
    logger.info("Calling prepare_data to read input files and infer relevant columns.")
    error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city = prepare_data(
        opportunities_file, demands_file, state, city
    )
    if error:
        logger.error("Error in prepare_data: %s", error)
        return error

    # Step 4: Call the refactored function to allocate demands using KNN
    logger.info("Invoking allocate_demands_knn with method=%s and k=%d", method, k)
    try:
        result_df = allocate_demands_knn(
            demands_gdf,
            opportunities_gdf,
            col_demand_id,
            col_name,
            col_city,
            k=k,
            method=method,
            city_name=city,
            num_threads=1  # Set to 1
        )
        logger.info("Allocation completed successfully. Number of rows in result: %d", len(result_df))
    except ValueError as ve:
        logger.exception("ValueError encountered during allocation.")
        raise HTTPException(status_code=400, detail=str(ve))
    
    # Step 5: Write the output file using the same logic for saving as CSV, geojson, etc.
    file_id = str(uuid.uuid4())
    OUTPUT_DIR = "/tmp/api_output/"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("Saving output to directory: %s", OUTPUT_DIR)

    if output_format == "csv":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.csv")
        result_df.to_csv(output_file, index=False)
        logger.info("Returning CSV file: %s", output_file)
        return FileResponse(output_file, media_type="text/csv", filename=os.path.basename(output_file))
    elif output_format == "geojson":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.geojson")
        result_df.to_json(output_file, index=False, orient="records")
        logger.info("Returning GeoJSON file: %s", output_file)
        return FileResponse(output_file, media_type="application/geo+json", filename=os.path.basename(output_file))
    elif output_format == "json":
        logger.info("Returning JSON response directly.")
        return JSONResponse(content=result_df.to_dict(orient="records"))
    else:
        logger.error("Invalid output format requested: %s", output_format)
        raise HTTPException(status_code=400, detail="Invalid output format. Choose 'csv', 'geojson', or 'json'.")
