import pandas as pd
import numpy as np
from libpysal.weights import KNN
from app.utils.utils import calculate_geodesic_distance

# Existing function for geodesic allocation
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

def allocate_demands_knn(demands_gdf, establishments_gdf, col_demand_id, col_name, col_city, k=1, target='establishments'):
    allocation = []

    # Reset indexes to ensure alignment
    demands_gdf = demands_gdf.reset_index(drop=True)
    establishments_gdf = establishments_gdf.reset_index(drop=True)

    # Choose the target for KNN (demands or establishments)
    if target == 'establishments':
        coords = np.array(list(zip(establishments_gdf.geometry.x, establishments_gdf.geometry.y)))
    elif target == 'demands':
        coords = np.array(list(zip(demands_gdf.geometry.x, demands_gdf.geometry.y)))
    else:
        return {"error": f"Invalid target for KNN: {target}. Must be 'establishments' or 'demands'."}

    # Check if the data is in the correct format
    if len(coords) == 0:
        return {"error": f"The coordinates for {target} are empty."}
    if coords.ndim != 2 or coords.shape[1] != 2:
        return {"error": f"The coordinates for {target} are not in the expected format (n, 2)."}

    # Create the KNN matrix (adjust k if necessary)
    if k > len(coords):
        k = len(coords)  # Adjust k if there are fewer points than k
    knn = KNN.from_array(coords, k=k)

    # Process the allocations
    for i, demand in demands_gdf.iterrows():
        demand_point = (demand.geometry.y, demand.geometry.x)
        
        # Define the nearest neighbors depending on the target
        if target == 'establishments':
            try:
                neighbors_idx = [int(idx) for idx in knn.neighbors[i]]  # Get the nearest neighbors of establishments
            except KeyError:
                neighbors_idx = [np.argmin([calculate_geodesic_distance(demand_point, (est.geometry.y, est.geometry.x)) for est in establishments_gdf.itertuples()])]
        else:  # If the target is "demands"
            try:
                neighbors_idx = [int(idx) for idx in knn.neighbors[i]]  # Get the nearest neighbors of demands
            except KeyError:
                neighbors_idx = [np.argmin([calculate_geodesic_distance(demand_point, (dem.geometry.y, dem.geometry.x)) for dem in demands_gdf.itertuples()])]

        closest_establishment = None
        shortest_distance = float('inf')

        # Iterate over the nearest neighbors and calculate the distance
        for neighbor_idx in neighbors_idx:
            if target == 'establishments':
                establishment = establishments_gdf.iloc[neighbor_idx]
                establishment_point = (establishment.geometry.y, establishment.geometry.x)
                distance = calculate_geodesic_distance(demand_point, establishment_point)
            else:
                neighbor_demand = demands_gdf.iloc[neighbor_idx]
                neighbor_point = (neighbor_demand.geometry.y, neighbor_demand.geometry.x)
                distance = calculate_geodesic_distance(demand_point, neighbor_point)

            if distance < shortest_distance:
                shortest_distance = distance
                closest_establishment = establishment if target == 'establishments' else None

        # Ensure "closest_establishment" is not null or empty
        allocation.append({
            'Sector_ID': demand[col_demand_id],
            'Establishment': getattr(closest_establishment, col_name) if closest_establishment is not None and not closest_establishment.empty else None,
            'Establishment_City': getattr(closest_establishment, col_city) if closest_establishment is not None and not closest_establishment.empty else None,
            'Distance': shortest_distance
        })

    result_df = pd.DataFrame(allocation).replace({np.nan: None, np.inf: None})
    return result_df
