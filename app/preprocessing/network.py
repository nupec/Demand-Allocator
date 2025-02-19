import sys
import pandas as pd
import geopandas as gpd
import pandana as pdna
import numpy as np
import osmnx as ox
from shapely.geometry import LineString
import warnings
from concurrent.futures import ThreadPoolExecutor
import logging

from app.preprocessing.utils import infer_column
from app.config import settings

# Basic logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore')


def compute_distance_matrix(demands_gdf, ubs_gdf, city_name=None, max_distance=50000, num_threads=1):
    """
    Computes the distance matrix between demands and opportunities using the Pandana network.
    Dynamically adjusts the buffer (even based on the 'AREA_KM2' column, if available)
    and maps the points to the network via OSMnx.
    """
    # Reset indices to ensure alignment
    demands_gdf = demands_gdf.reset_index(drop=True)
    ubs_gdf = ubs_gdf.reset_index(drop=True)

    # Convert to the standard coordinate reference system (EPSG:4326)
    demands_gdf = demands_gdf.to_crs(epsg=4326)
    ubs_gdf = ubs_gdf.to_crs(epsg=4326)

    # Filter by city if provided
    if city_name:
        demands_gdf = demands_gdf[demands_gdf['NM_MUN'].str.upper() == city_name.upper()]
        ubs_gdf = ubs_gdf[ubs_gdf['MUNICÍPIO'].str.upper() == city_name.upper()]

    # Calculate centroids for demands if geometries are not points
    if demands_gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon']).any():
        demands_gdf['centroid'] = demands_gdf.geometry.centroid
    else:
        demands_gdf['centroid'] = demands_gdf.geometry

    # Determine the buffer size based on the AREA_KM2 column (if available)
    min_buffer_deg = 0.1  # Approximately 11 km
    max_buffer_deg = 1.0  # Approximately 111 km
    if 'AREA_KM2' in demands_gdf.columns and not demands_gdf['AREA_KM2'].isnull().all():
        area_km2 = demands_gdf['AREA_KM2'].mean()
        radius_km = np.sqrt(area_km2 / np.pi)
        radius_deg = radius_km / 111  # Approximation: 1 degree ≈ 111 km
        buffer_size = radius_deg * 2  # Adjustable multiplicative factor
        buffer_size = max(min_buffer_deg, min(buffer_size, max_buffer_deg))
        logger.info(f"Buffer calculated based on AREA_KM2: {buffer_size} degrees")
    else:
        buffer_size = 0.1
        logger.info("Using default buffer: 0.1 degrees")

    # Combine the geometries of demands and opportunities to define the area of interest
    combined_geom = demands_gdf.unary_union.union(ubs_gdf.unary_union)

    max_attempts = 3
    attempt = 0

    while attempt < max_attempts:
        # Define the area of interest with the current buffer
        area_of_interest = combined_geom.buffer(buffer_size)

        try:
            logger.info(f"Attempt {attempt+1}: Downloading road network with buffer {buffer_size} degrees")
            graph = ox.graph_from_polygon(area_of_interest, network_type='drive', simplify=True)
        except Exception as e:
            logger.error(f"Error downloading the network: {e}. Increasing buffer and trying again.")
            buffer_size = min(buffer_size * 1.5, max_buffer_deg)
            attempt += 1
            continue

        # Convert the graph to GeoDataFrames
        nodes, edges = ox.graph_to_gdfs(graph, nodes=True, edges=True)
        nodes = nodes.reset_index()
        edges = edges.reset_index()

        # Create the Pandana network with appropriate type conversions
        x = nodes['x'].values
        y = nodes['y'].values
        mapping = dict(zip(nodes['osmid'], nodes.index))
        # Define the integer type for nodes according to the platform:
        # On Windows, "long" is 32 bits; on other systems, it is usually 64 bits.
        if sys.platform.startswith("win"):
            node_dtype = np.int32
        else:
            node_dtype = np.intp

        from_nodes = edges['u'].map(mapping).astype(node_dtype)
        to_nodes = edges['v'].map(mapping).astype(node_dtype)
        # Convert edge weights to a DataFrame with dtype float64
        edge_weights = pd.DataFrame(edges['length'].astype(np.float64))

        network = pdna.Network(x, y, from_nodes, to_nodes, edge_weights)

        # Map demand and opportunity points to the nearest nodes in the network
        demand_coords = np.array(list(zip(demands_gdf['centroid'].x, demands_gdf['centroid'].y)))
        ubs_coords = np.array(list(zip(ubs_gdf.geometry.x, ubs_gdf.geometry.y)))

        demand_nodes_array = network.get_node_ids(demand_coords[:, 0], demand_coords[:, 1])
        ubs_nodes_array = network.get_node_ids(ubs_coords[:, 0], ubs_coords[:, 1])

        # Check for invalid nodes (-1 indicates that the point was not mapped)
        invalid_demand_nodes = np.where(demand_nodes_array == -1)[0]
        invalid_ubs_nodes = np.where(ubs_nodes_array == -1)[0]

        if len(invalid_demand_nodes) > 0 or len(invalid_ubs_nodes) > 0:
            logger.warning(f"{len(invalid_demand_nodes)} demands and {len(invalid_ubs_nodes)} opportunities are outside the network.")
            buffer_size = min(buffer_size * 1.5, max_buffer_deg)
            logger.info(f"Increasing buffer to {buffer_size} degrees and rebuilding the network.")
            attempt += 1
            continue
        else:
            logger.info("All points were successfully mapped to the network.")
            break

    if attempt == max_attempts:
        logger.error("Unable to map all points to the network after the maximum number of attempts.")
        # Remove invalid points
        if len(invalid_demand_nodes) > 0:
            demands_gdf = demands_gdf.drop(demands_gdf.index[invalid_demand_nodes]).reset_index(drop=True)
            demand_coords = np.delete(demand_coords, invalid_demand_nodes, axis=0)
            demand_nodes_array = np.delete(demand_nodes_array, invalid_demand_nodes)
        if len(invalid_ubs_nodes) > 0:
            ubs_gdf = ubs_gdf.drop(ubs_gdf.index[invalid_ubs_nodes]).reset_index(drop=True)
            ubs_coords = np.delete(ubs_coords, invalid_ubs_nodes, axis=0)
            ubs_nodes_array = np.delete(ubs_nodes_array, invalid_ubs_nodes)

    # Create pandas Series with node IDs, aligned with the updated DataFrames
    demand_nodes = pd.Series(demand_nodes_array, index=demands_gdf.index)
    ubs_nodes = pd.Series(ubs_nodes_array, index=ubs_gdf.index)

    # Precompute routes up to the maximum distance
    network.precompute(max_distance)

    # Calculate the distance matrix using parallelism
    num_demands = len(demand_nodes)
    num_ubs = len(ubs_nodes)
    distances = np.empty((num_demands, num_ubs), dtype=np.float32)

    def compute_row(i):
        orig_node = demand_nodes.iloc[i]
        orig_nodes = np.full(num_ubs, orig_node, dtype=int)
        distances_row = network.shortest_path_lengths(orig_nodes, ubs_nodes.values)
        return i, distances_row

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(compute_row, i) for i in range(num_demands)]
        for future in futures:
            i, distances_row = future.result()
            distances[i, :] = distances_row

    distances[np.isinf(distances)] = np.nan

    # Create a DataFrame for the distance matrix
    distance_df = pd.DataFrame(distances, index=demands_gdf.index, columns=ubs_gdf.index)

    # Infer the identifier column names
    col_demand_id = infer_column(demands_gdf, settings.DEMAND_ID_POSSIBLE_COLUMNS)
    col_name = infer_column(ubs_gdf, settings.NAME_POSSIBLE_COLUMNS)

    demands_ids = demands_gdf[col_demand_id].values
    ubs_names = ubs_gdf[col_name].values

    # Update the DataFrame indices to use the actual identifiers
    distance_df.index = demands_ids
    distance_df.columns = ubs_names

    # Debug log for zero distances
    for i, row in distance_df.iterrows():
        for j, dist in row.items():
            if dist == 0:
                logger.debug(f"Zero distance found between demand {i} and opportunity {j}")

    return distance_df, network, graph, nodes, edges, demand_nodes, ubs_nodes
