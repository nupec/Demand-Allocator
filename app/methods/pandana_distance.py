import pandas as pd
import numpy as np
from unidecode import unidecode
from app.preprocessing.network import compute_distance_matrix
from app.methods.geodesic_distance import calculate_geodesic_distance

def pandana_distance_matrix(
    demands_gdf,
    opportunities_gdf,
    col_demand_id,
    col_name,
    city_name=None,
    max_distance=50000,
    num_threads=1
):
    
    """
    Returns a distance matrix (DataFrame) using 'compute_distance_matrix' (pandana network).
      - index = demand ID
      - columns = opportunity name
      - values = distance (km)
    """
    distance_df, network, graph, nodes, edges, demand_nodes, ubs_nodes = compute_distance_matrix(
        demands_gdf,
        opportunities_gdf,
        city_name=city_name,
        max_distance=max_distance,
        num_threads=num_threads
    )
    distance_df = distance_df / 1000.0

    # Note: Fallback implemented for cases where real distances are zero; if distance == 0, use geodesic distance
    for demand_id in distance_df.index:
        row = distance_df.loc[demand_id]
        zeros = row[row == 0.0].index # columns with zero distance
        if len(zeros) > 0:
            # Get the geometry of the demand
            demand_row = demands_gdf[demands_gdf[col_demand_id] == demand_id].iloc[0]
            demand_point = (demand_row.geometry.y, demand_row.geometry.x)

            for opp_name in zeros:
                opportunities_row = opportunities_gdf[opportunities_gdf[col_name].apply(lambda x: unidecode(x).lower()) 
                                                      == unidecode(opp_name).lower()]
                if not opportunities_row.empty:
                    opp = opportunities_row.iloc[0]
                    opp_point = (opp.geometry.y, opp.geometry.x)
                    dist_geo = calculate_geodesic_distance(demand_point, opp_point)
                    distance_df.loc[demand_id, opp_name] = dist_geo
    
    return distance_df
