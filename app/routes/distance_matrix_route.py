from fastapi import APIRouter, UploadFile, Query
from typing import Optional
from fastapi.responses import StreamingResponse, JSONResponse
from app.network_analysis.network import compute_distance_matrix
from app.allocation.common import prepare_data
import osmnx as ox

router = APIRouter()


@router.post("/distance_matrix/")
def get_distance_matrix(
    establishments_file: UploadFile,
    demands_file: UploadFile,
    state: Optional[str] = Query(None, description="State (optional)"),
    city: Optional[str] = Query(None, description="City (optional)")
):
    # Prepare data using the prepare_data function
    error, demands_gdf, establishments_gdf, col_demand_id, col_name, col_city = prepare_data(
        establishments_file,
        demands_file,
        state,
        city
    )

    if error:
        return error

    # Calculate the distance matrix
    distance_df, network, graph, nodes, edges, demand_nodes, ubs_nodes = compute_distance_matrix(
        demands_gdf,
        establishments_gdf,
        city_name=city
    )

    # Convert the distance DataFrame to a JSON-serializable format
    distance_json = distance_df.to_dict()

    return JSONResponse(content=distance_json)
