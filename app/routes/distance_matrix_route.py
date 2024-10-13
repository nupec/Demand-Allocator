from fastapi import APIRouter, UploadFile, Query, HTTPException
from typing import Optional
from fastapi.responses import FileResponse
from app.network_analysis.network import compute_distance_matrix, plot_shortest_route
from app.allocation.common import prepare_data
import os
import uuid

router = APIRouter()

# Define a directory to store generated files
OUTPUT_DIR = "/tmp/api_output/"  

# Ensure the directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


@router.post("/distance_matrix/")
def get_distance_matrix(
    establishments_file: UploadFile,
    demands_file: UploadFile,
    state: Optional[str] = Query(None, description="State (optional)"),
    city: Optional[str] = Query(None, description="City (optional)"),
    num_threads: int = Query(1, description="Number of threads to use")
):
    # Limit the number of threads based on CPU count
    max_threads = os.cpu_count() or 4  # Default to 4 if os.cpu_count() returns None
    if num_threads < 1 or num_threads > max_threads:
        raise HTTPException(
            status_code=400,
            detail=f"The number of threads must be between 1 and {max_threads}."
        )

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
        city_name=city,
        num_threads=num_threads  # Passing the number of threads
    )

    # Generate a unique filename to avoid conflicts
    file_id = str(uuid.uuid4())
    output_file = os.path.join(OUTPUT_DIR, f"distance_matrix_{file_id}.json")

    # Convert the DataFrame to JSON and save it to the file
    distance_df.to_json(output_file)

    # Return the file for download directly
    return FileResponse(output_file, media_type="application/json", filename=f"distance_matrix_{file_id}.json")


@router.post("/plot_shortest_route/")
def get_shortest_route_plot(
    establishments_file: UploadFile,
    demands_file: UploadFile,
    state: Optional[str] = Query(None, description="State (optional)"),
    city: Optional[str] = Query(None, description="City (optional)"),
    num_threads: int = Query(1, description="Number of threads to use")
):
    # Limit the number of threads based on CPU count
    max_threads = os.cpu_count() or 4  # Default to 4 if os.cpu_count() returns None
    if num_threads < 1 or num_threads > max_threads:
        raise HTTPException(
            status_code=400,
            detail=f"The number of threads must be between 1 and {max_threads}."
        )

    # Prepare the data using the prepare_data function
    error, demands_gdf, establishments_gdf, col_demand_id, col_name, col_city = prepare_data(
        establishments_file,
        demands_file,
        state,
        city
    )

    if error:
        return error

    # Calculate the distance matrix
    distance_df, network, graph, nodes, edges, demand_nodes, establishment_nodes = compute_distance_matrix(
        demands_gdf,
        establishments_gdf,
        city_name=city,
        num_threads=num_threads  # Passing the number of threads
    )

    # Select the first demand point
    demand_index = demands_gdf.index[0]

    # Find the nearest establishment for the selected demand point
    distances = distance_df.loc[demand_index]
    nearest_establishment_index = distances.idxmin()

    # Get the corresponding demand and establishment points
    demand_point = demands_gdf.loc[[demand_index]]
    establishment_point = establishments_gdf.loc[[nearest_establishment_index]]

    # Get the Pandana node indices
    demand_node_index = demand_nodes[demands_gdf.index.get_loc(demand_index)]
    establishment_node_index = establishment_nodes[establishments_gdf.index.get_loc(nearest_establishment_index)]

    # Map the Pandana node indices back to osmid values
    demand_node_osmid = nodes.loc[demand_node_index, 'osmid']
    establishment_node_osmid = nodes.loc[establishment_node_index, 'osmid']

    # Generate a unique filename to avoid conflicts
    file_id = str(uuid.uuid4())
    output_image = os.path.join(OUTPUT_DIR, f"shortest_route_{file_id}.png")

    # Pass 'establishments_gdf' as 'all_ubs_points' and save the plot to a file
    buf = plot_shortest_route(
        graph,
        nodes,
        edges,
        demand_point,
        establishment_point,
        demand_node_osmid,
        establishment_node_osmid,
        establishments_gdf  # Passing all UBS points
    )

    # Check if buf is None
    if buf is None:
        raise HTTPException(
            status_code=400,
            detail="No valid route found between the selected points."
        )

    with open(output_image, 'wb') as f:
        f.write(buf.getbuffer())

    # Return the image for download
    return FileResponse(output_image, media_type="image/png", filename=f"shortest_route_{file_id}.png")
