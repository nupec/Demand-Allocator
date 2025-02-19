import pandas as pd
from geopy.distance import geodesic  # Import necessário para o cálculo de distâncias geodésicas

def calculate_geodesic_distance(point1, point2):
    """
    Calculate the geodesic distance between two points in kilometers.
    """
    return geodesic(point1, point2).kilometers

def geodesic_method(demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city):
    """
    Allocate demands to the closest opportunities based on geodesic distance.
    """
    allocation = []

    for i, demand in demands_gdf.iterrows():
        demand_point = (demand.geometry.y, demand.geometry.x)
        shortest_distance = float('inf')
        closest_opportunity = None

        for _, opportunity in opportunities_gdf.iterrows():
            opportunity_point = (opportunity.geometry.y, opportunity.geometry.x)
            distance = calculate_geodesic_distance(demand_point, opportunity_point)

            if distance < shortest_distance:
                shortest_distance = distance
                closest_opportunity = opportunity

        allocation.append({
            'ID_Sector': demand[col_demand_id],
            'Origin_Lat': demand_point[0],
            'Origin_Lon': demand_point[1],
            'Opportunities_Name': closest_opportunity[col_name] if closest_opportunity is not None else None,
            'Destination_Lat': closest_opportunity.geometry.y if closest_opportunity is not None else None,
            'Destination_Lon': closest_opportunity.geometry.x if closest_opportunity is not None else None,
            'Distance': shortest_distance,
        })

    return pd.DataFrame(allocation)
