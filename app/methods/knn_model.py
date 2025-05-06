import logging
import pandas as pd
import os
from fastapi import HTTPException


from app.methods.geodesic_distance import geodesic_distance_matrix
from app.methods.pandana_distance import pandana_distance_matrix
from app.methods.knn_allocation import select_knn_from_distance_matrix, join_knn_with_geometries

logger = logging.getLogger(__name__)

def allocate_demands_knn(
    demands_gdf,
    opportunities_gdf,
    col_demand_id,
    col_name,
    col_city,
    col_state,  
    k=1,
    method="geodesic",
    city_name=None,
    num_threads: int = 1
):
    """
    Allocates demands using K-nearest neighbors (KNN).
    """
    if num_threads < 1:
        num_threads = os.cpu_count()
    logger.info("Starting allocate_demands_knn with method='%s', k=%d, city_name=%s, num_threads=%d", method, k, city_name, num_threads)

    if method == "geodesic":
        logger.info("Using geodesic distance matrix.")
        dist_df = geodesic_distance_matrix(demands_gdf, opportunities_gdf, col_demand_id, col_name)
    elif method == "pandana_real_distance":
        logger.info("Using pandana real distance matrix.")
        dist_df = pandana_distance_matrix(
            demands_gdf,
            opportunities_gdf,
            col_demand_id,
            col_name,
            city_name=city_name,
            num_threads=num_threads
        )
    else:
        logger.error("Invalid method specified: '%s'", method)
        raise HTTPException(status_code=400, detail="Invalid method. Use 'geodesic' or 'pandana_real_distance'.")

    logger.debug("Distance matrix shape: %s x %s", dist_df.shape[0], dist_df.shape[1])

    # Select the K nearest neighbors
    logger.info("Selecting K=%d nearest neighbors for each demand.", k)
    knn_df = select_knn_from_distance_matrix(dist_df, k=k)
    logger.info("KNN selection completed. Rows in knn_df: %d", len(knn_df))

    # Join with geometry data
    logger.info("Joining KNN results with original GeoDataFrames.")
    result_df = join_knn_with_geometries(knn_df, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city, col_state)
    logger.info("Geometry join complete. Rows in result_df: %d", len(result_df))

    # Some aggregated stats
    logger.info("Calculating distance statistics by opportunity_name.")
    stats = result_df.groupby('opportunity_name')['distance_km'].agg(
        distance_mean='mean',
        distance_variance=lambda x: x.var(ddof=0)
    ).reset_index()
    result_df = result_df.merge(stats, on='opportunity_name', how='left')

    # Reordenar as colunas conforme a ordem desejada
    desired_order = [
        'demand_id',
        'Destination_State',
        'Destination_City',
        'opportunity_name',
        'Origin_Lat',
        'Origin_Lon',
        'Destination_Lat',
        'Destination_Lon',
        'distance_km',
        'distance_mean',
        'distance_variance'
    ]
    result_df = result_df[desired_order]

    logger.info("allocate_demands_knn completed successfully.")
    return result_df
