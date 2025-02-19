from fastapi import HTTPException
import pandas as pd

from app.methods.geodesic_distance import geodesic_distance_matrix
from app.methods.pandana_distance import pandana_distance_matrix
from app.methods.knn_allocation import select_knn_from_distance_matrix, join_knn_with_geometries

def allocate_demands_knn(
    demands_gdf,
    opportunities_gdf,
    col_demand_id,
    col_name,
    col_city,
    k=1,
    method="geodesic",
    city_name=None,
    num_threads=1
):
    """
    Allocates demands using KNN (k-nearest neighbors).
    - method: "geodesic" or "pandana_real_distance"
    - k: number of neighbors
    - city_name and num_threads (optional for pandana)
    """

    if method == "geodesic":
        # Step 1: Obtain the geodesic distance matrix
        dist_df = geodesic_distance_matrix(demands_gdf, opportunities_gdf, col_demand_id, col_name)
    elif method == "pandana_real_distance":
        # Step 1: Obtain the distance matrix using pandana
        dist_df = pandana_distance_matrix(
            demands_gdf,
            opportunities_gdf,
            col_demand_id,
            col_name,
            city_name=city_name,
            num_threads=num_threads
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid method. Use 'geodesic' or 'pandana_real_distance'.")

    # Step 2: Select the K nearest neighbors
    knn_df = select_knn_from_distance_matrix(dist_df, k=k)

    # Step 3: Join with geometry data (lat/lon) and return the result
    result_df = join_knn_with_geometries(knn_df, demands_gdf, opportunities_gdf, col_demand_id, col_name)

    # Note: Here, 'result_df' will have 'k' rows for each demand (unless 'k' exceeds 
    # the number of available opportunities).
    # If you want to return ONLY the 1st neighbor, simply set k=1 or filter at the end.
    return result_df
