import pandas as pd
import numpy as np

from libpysal.weights import KNN,  knnW_from_array
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

def allocate_demands_knn(demands_gdf, establishments_gdf, col_demand_id, col_name, col_city, k=1):
    allocation = []

    demands_coords = np.array(list(zip(demands_gdf.geometry.x, demands_gdf.geometry.y)))
    establishments_coords = np.array(list(zip(establishments_gdf.geometry.x, establishments_gdf.geometry.y)))
    
    knn = knnW_from_array(establishments_coords, k=k)
    
    for i, demand in enumerate(demands_gdf.itertuples(), 1):
        demand_coord = np.array([demand.geometry.x, demand.geometry.y])
        
        neighbors_idx = knn.neighbors[i]
        closest_establishments = establishments_gdf.iloc[neighbors_idx]
        
        closest_establishment = closest_establishments.iloc[0]  
        
        allocation.append({
            'Sector_ID': getattr(demand, col_demand_id),
            'Establishment': getattr(closest_establishment, col_name),
            'Establishment_City': getattr(closest_establishment, col_city),
            'Distance': calculate_geodesic_distance(
                (demand.geometry.y, demand.geometry.x), 
                (closest_establishment.geometry.y, closest_establishment.geometry.x)
            )
        })

    return pd.DataFrame(allocation)
