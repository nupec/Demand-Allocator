import pandas as pd
from geopy.distance import geodesic

def calculate_geodesic_distance(point1, point2):
    """
    Calculate the geodesic distance between two points (lat, lon) in kilometers.
    """
    return geodesic(point1, point2).kilometers


def geodesic_distance_matrix(demands_gdf, opportunities_gdf, col_demand_id, col_name):
    """
    Returns a distance matrix (DataFrame) of geodesic distances between each demand and each opportunity.
      - index = demand ID
      - columns = opportunity name
      - values = distance (km)
    """
    # Reset indices to ensure proper alignment
    demands_gdf = demands_gdf.reset_index(drop=True)
    opportunities_gdf = opportunities_gdf.reset_index(drop=True)

    # Extract unique demand IDs and opportunity names to form the final DataFrame
    demand_ids = demands_gdf[col_demand_id].unique()
    opportunity_names = opportunities_gdf[col_name].unique()

    # Create an empty DataFrame with demands as the index and opportunities as columns
    distance_df = pd.DataFrame(
        index=demand_ids,
        columns=opportunity_names,
        dtype=float
    )

    # Loop through each demand to fill in the distance matrix
    for i, demand in demands_gdf.iterrows():
        demand_id = demand[col_demand_id]
        demand_point = (demand.geometry.y, demand.geometry.x)

        for j, opportunity in opportunities_gdf.iterrows():
            opp_name = opportunity[col_name]
            opp_point = (opportunity.geometry.y, opportunity.geometry.x)

            dist = calculate_geodesic_distance(demand_point, opp_point)
            distance_df.loc[demand_id, opp_name] = dist

    return distance_df
