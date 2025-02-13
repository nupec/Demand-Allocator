import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
import matplotlib.pyplot as plt
from shapely.geometry import LineString
import warnings
import io
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings('ignore')


def compute_distance_matrix(demands_gdf, ubs_gdf, city_name=None, max_distance=50000, num_threads=1):
    # Convert to the correct CRS
    demands_gdf = demands_gdf.to_crs(epsg=4326)
    ubs_gdf = ubs_gdf.to_crs(epsg=4326)


    # Filter by city if provided
    if city_name:
        demands_gdf = demands_gdf[demands_gdf['NM_MUN'].str.upper() == city_name.upper()]
        ubs_gdf = ubs_gdf[ubs_gdf['MUNICÍPIO'].str.upper() == city_name.upper()]

    # Calculate centroids of the demands
    demands_gdf['centroid'] = demands_gdf.geometry.centroid

    # Combine demands and establishments geometries
    combined_geom = demands_gdf.unary_union.union(ubs_gdf.unary_union)

    # Define area of interest with an increased buffer if necessary
    area_of_interest = combined_geom.buffer(0.05)  # Increased buffer to 0.05 degrees

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

    def compute_row(i):
        orig_nodes = np.full(num_ubs, demand_nodes[i], dtype=int)
        distances_row = network.shortest_path_lengths(orig_nodes, ubs_nodes)
        return i, distances_row

    # Use ThreadPoolExecutor to parallelize the loop
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(compute_row, i) for i in range(num_demands)]
        for future in futures:
            i, distances_row = future.result()
            distances[i, :] = distances_row

    # Create distance DataFrame
    distance_df = pd.DataFrame(distances, index=demands_gdf.index, columns=ubs_gdf.index)

    return distance_df, network, graph, nodes, edges, demand_nodes, ubs_nodes


def plot_shortest_route(graph, nodes, edges, demand_point, establishment_point, demand_node_osmid, establishment_node_osmid, all_ubs_points):
    # Get the shortest route using osmid values
    try:
        shortest_route = ox.shortest_path(graph, demand_node_osmid, establishment_node_osmid, weight='length')
    except Exception as e:
        print(f"Error finding shortest path: {e}")
        return None  # Handle the case where no route is found

    if not shortest_route or len(shortest_route) < 2:
        print("No valid route found between the selected points.")
        return None  # Handle the case where no route is found

    # Plot the map with the shortest route
    fig, ax = plt.subplots(figsize=(24, 24))  # Increase figure size for higher resolution
    fig.patch.set_facecolor('black')
    ax.set_facecolor('black')

    # Plot the street network edges
    edges.plot(ax=ax, linewidth=0.5, color='white', alpha=0.5)

    # Plot the nodes (network intersections)
    nodes.plot(ax=ax, color='white', markersize=1, alpha=0.8)

    # Plot all UBS points in red
    all_ubs_points.plot(ax=ax, color='red', markersize=20, label='All UBS')

    # Plot the shortest route
    route_nodes = nodes.set_index('osmid').loc[shortest_route]
    route_line = LineString(route_nodes[['x', 'y']].values)
    gpd.GeoSeries([route_line]).plot(ax=ax, linewidth=2, color='yellow', alpha=0.7)

    # Plot the selected establishment (UBS) in yellow
    establishment_point.plot(ax=ax, color='yellow', markersize=50, label='Nearest Establishment')

    # Plot the demand point in blue
    demand_point.plot(ax=ax, color='blue', markersize=50, label='Demand Point')

    plt.axis('off')

    # Save the plot to an in-memory buffer with the highest resolution possible
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', facecolor=fig.get_facecolor(), dpi=1300)
    buf.seek(0)
    plt.close(fig)
    return buf
