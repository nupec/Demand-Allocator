import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import matplotlib.pyplot as plt
import warnings
import io 

warnings.filterwarnings('ignore')


def compute_distance_matrix(demands_gdf, ubs_gdf, city_name=None, max_distance=50000):
    # Convert to the correct CRS
    demands_gdf = demands_gdf.to_crs(epsg=4326)
    ubs_gdf = ubs_gdf.to_crs(epsg=4326)

    # Filter by city if provided
    if city_name:
        demands_gdf = demands_gdf[demands_gdf['NM_MUN'].str.upper() == city_name.upper()]
        ubs_gdf = ubs_gdf[ubs_gdf['MUNICÍPIO'].str.upper() == city_name.upper()]

    # Calculate centroids of the demands
    demands_gdf['centroid'] = demands_gdf.geometry.centroid

    # Define area of interest
    area_of_interest = demands_gdf.unary_union.buffer(0.01)

    # Download street network
    graph = ox.graph_from_polygon(area_of_interest, network_type='drive', simplify=True)

    # Convert graph to DataFrames
    nodes, edges = ox.graph_to_gdfs(graph, nodes=True, edges=True)
    edges = edges.reset_index()
    nodes = nodes.reset_index()

    # Create Pandana network
    x = nodes['x'].values
    y = nodes['y'].values
    from_nodes = edges['u'].map(dict(zip(nodes['osmid'], nodes.index))).astype(np.int32)
    to_nodes = edges['v'].map(dict(zip(nodes['osmid'], nodes.index))).astype(np.int32)
    edge_weights = pd.DataFrame(edges['length'])

    network = pdna.Network(x, y, from_nodes, to_nodes, edge_weights)

    # Map demand and supply points
    demand_coords = np.array(list(zip(demands_gdf['centroid'].x, demands_gdf['centroid'].y)))
    ubs_coords = np.array(list(zip(ubs_gdf.geometry.x, ubs_gdf.geometry.y)))

    demand_nodes = network.get_node_ids(demand_coords[:, 0], demand_coords[:, 1])
    ubs_nodes = network.get_node_ids(ubs_coords[:, 0], ubs_coords[:, 1])

    # Precompute routes
    network.precompute(max_distance)

    # Calculate the distance matrix
    num_demands = len(demand_nodes)
    num_ubs = len(ubs_nodes)
    distances = np.empty((num_demands, num_ubs), dtype=np.float32)

    for i in range(num_demands):
        orig_nodes = np.full(num_ubs, demand_nodes[i], dtype=int)
        distances[i, :] = network.shortest_path_lengths(orig_nodes, ubs_nodes)

    # Create distance DataFrame
    distance_df = pd.DataFrame(distances, index=demands_gdf.index, columns=ubs_gdf.index)

    return distance_df, network, graph, nodes, edges, demand_nodes, ubs_nodes

