import pandas as pd
from unidecode import unidecode
from app.preprocessing.network import compute_distance_matrix
from app.methods.geodesic_method import calculate_geodesic_distance


def real_distance_pandana_method(
    demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city, city_name=None, max_distance=50000, num_threads=1
):
    """
    Allocate demands to opportunities based on real distances calculated via Pandana.
    """
    # Compute the distance matrix
    distance_df, network, graph, nodes, edges, demand_nodes, ubs_nodes = compute_distance_matrix(
        demands_gdf, opportunities_gdf, city_name=city_name, max_distance=max_distance, num_threads=num_threads
    )

    allocation = []
    # Convert distances from meters to kilometers
    distance_df = distance_df / 1000

    # Allocate each demand to the closest opportunity
    for demand_id in distance_df.index:
        distances = distance_df.loc[demand_id].dropna()
        if distances.empty:
            print(f"No valid paths for demand {demand_id}.")
            continue

        shortest_distance = distances.min()
        opportunities_name = distances.idxmin()

        demand_row = demands_gdf[demands_gdf[col_demand_id] == demand_id].iloc[0]
        demand_point = (demand_row.geometry.y, demand_row.geometry.x)

        opportunities_gdf['col_name_normalized'] = opportunities_gdf[col_name].apply(lambda x: unidecode(x).lower())
        opportunities_row = opportunities_gdf[
            opportunities_gdf['col_name_normalized'] == unidecode(opportunities_name).lower()
        ]

        if opportunities_row.empty:
            print(f"Opportunity '{opportunities_name}' not found.")
            continue

        opportunities_row = opportunities_row.iloc[0]
        opportunities_point = (opportunities_row.geometry.y, opportunities_row.geometry.x)

        if shortest_distance == 0:
            shortest_distance = calculate_geodesic_distance(demand_point, opportunities_point)
            print(f"Fallback to geodesic distance: {shortest_distance} km for demand {demand_id}") ### verificar essa viabilidade com o felipe para casos com distancias com zero

        allocation.append({
            'ID_Sector': demand_id,
            'Origin_Lat': demand_point[0],
            'Origin_Lon': demand_point[1],
            'Opportunities_Name': opportunities_name,
            'Destination_Lat': opportunities_point[0],
            'Destination_Lon': opportunities_point[1],
            'Distance': shortest_distance,
        })  

    return pd.DataFrame(allocation)
