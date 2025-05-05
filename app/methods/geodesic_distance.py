import logging
import pandas as pd
from geopy.distance import geodesic

logger = logging.getLogger(__name__)

def calculate_geodesic_distance(point1, point2):
    """
    Calculate the geodesic distance between two points in kilometers.
    """
    return geodesic(point1, point2).kilometers

def geodesic_distance_matrix(demands_gdf, opportunities_gdf, col_demand_id, col_name):
    logger.info("Constructing geodesic distance matrix...")
    demands_gdf = demands_gdf.reset_index(drop=True)
    opportunities_gdf = opportunities_gdf.reset_index(drop=True)

    demand_ids = demands_gdf[col_demand_id].unique()
    opportunity_names = opportunities_gdf[col_name].unique()
    logger.debug("Number of unique demands: %d, opportunities: %d", len(demand_ids), len(opportunity_names))

    distance_df = pd.DataFrame(index=demand_ids, columns=opportunity_names, dtype=float)

    for i, demand in demands_gdf.iterrows():
        demand_id = demand[col_demand_id]
        demand_point = (demand.geometry.y, demand.geometry.x)
        for j, opportunity in opportunities_gdf.iterrows():
            opp_name = opportunity[col_name]
            opp_point = (opportunity.geometry.y, opportunity.geometry.x)
            dist = calculate_geodesic_distance(demand_point, opp_point)
            distance_df.loc[demand_id, opp_name] = dist

    logger.info("Geodesic distance matrix constructed with shape %s", distance_df.shape)
    return distance_df
