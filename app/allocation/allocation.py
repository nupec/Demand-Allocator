import pandas as pd
from app.utils.utils import calculate_geodesic_distance

def allocate_demands(demands_gdf, establishments_gdf, col_demand_id, col_name, col_city):
    allocation = []

    for i, demand in enumerate(demands_gdf.itertuples(), 1):
        demand_point = (demand.geometry.y, demand.geometry.x)
        shortest_distance = float('inf')
        closest_establishment = None

        for establishment in establishments_gdf.itertuples():
            establishment_point = (establishment.geometry.y, establishment.geometry.x)
            distance = calculate_geodesic_distance(demand_point, establishment_point)
            if distance < shortest_distance:
                shortest_distance = distance
                closest_establishment = establishment

        allocation.append({
            'Sector_ID': getattr(demand, col_demand_id),
            'Establishment': getattr(closest_establishment, col_name) if closest_establishment else None,
            'Establishment_City': getattr(closest_establishment, col_city) if closest_establishment else None,
            'Distance': shortest_distance
        })

    return pd.DataFrame(allocation)
