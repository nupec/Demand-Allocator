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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore')

def compute_distance_matrix(demands_gdf, ubs_gdf, city_name=None, max_distance=50000, num_threads=1):
    """
    1. Attempts to download the network from the Kumi Systems endpoint.
    2. If it fails, attempts the official OpenStreetMap endpoint.
    3. If both fail, increases the buffer and tries again, until 'max_attempts' is reached.
    4. The timeout is increased to 2500s (about 41 minutes).
    """

    # Define timeout once (applies to all attempts).
    ox.settings.timeout = 2500

    # List of endpoints to try on each attempt.
    endpoints = [
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass-api.de/api/interpreter"
    ]

    demands_gdf = demands_gdf.reset_index(drop=True)
    ubs_gdf = ubs_gdf.reset_index(drop=True)

    demands_gdf = demands_gdf.to_crs(epsg=4326)
    ubs_gdf = ubs_gdf.to_crs(epsg=4326)

    if city_name:
        demands_gdf = demands_gdf[demands_gdf['NM_MUN'].str.upper() == city_name.upper()]
        ubs_gdf = ubs_gdf[ubs_gdf['MUNICÍPIO'].str.upper() == city_name.upper()]

    # Initial buffer (in degrees) to expand if points are outside the network.
    buffer_size = 0.1
    combined_geom = demands_gdf.unary_union.union(ubs_gdf.unary_union)

    max_attempts = 3
    attempt = 0

    graph = None
    demand_nodes_array = None
    ubs_nodes_array = None
    invalid_demand_nodes = np.array([])
    invalid_ubs_nodes = np.array([])

    while attempt < max_attempts:
        area_of_interest = combined_geom.buffer(buffer_size)

        # Try each endpoint in order until the graph is successfully downloaded or the list is exhausted.
        graph = None
        for endpoint in endpoints:
            try:
                # Set the current endpoint.
                ox.settings.overpass_endpoint = endpoint

                logger.info(
                    f"Attempt {attempt+1} - Endpoint: {endpoint} "
                    f"(Buffer of {buffer_size} degrees)"
                )

                graph = ox.graph_from_polygon(
                    area_of_interest,
                    network_type='drive',
                    simplify=True
                )
                # If successful, break out of the endpoints loop.
                break

            except Exception as e:
                logger.warning(
                    f"Failed to download network from {endpoint}: {e}. "
                    "Trying next endpoint..."
                )

        # If 'graph' is still None, it means that both endpoints failed.
        if graph is None:
            logger.error("Failure on all endpoints in this attempt.")
            buffer_size = min(buffer_size * 1.5, 1.0)
            attempt += 1
            continue

        # Converting the graph into GeoDataFrames.
        nodes, edges = ox.graph_to_gdfs(graph, nodes=True, edges=True)
        nodes = nodes.reset_index()
        edges = edges.reset_index()

        # Adjust type for cross-platform compatibility.
        if sys.platform.startswith("win"):
            node_dtype = np.int32
        else:
            node_dtype = np.intp

        from_nodes = edges['u'].map(dict(zip(nodes['osmid'], nodes.index))).astype(node_dtype)
        to_nodes = edges['v'].map(dict(zip(nodes['osmid'], nodes.index))).astype(node_dtype)
        edge_weights = pd.DataFrame(edges['length'].astype(np.float64))

        # Building the Pandana network.
        network = pdna.Network(
            nodes['x'].values,
            nodes['y'].values,
            from_nodes,
            to_nodes,
            edge_weights
        )

        # Extracting coordinates.
        demand_coords = np.array(list(zip(demands_gdf['geometry'].x, demands_gdf['geometry'].y)))
        ubs_coords = np.array(list(zip(ubs_gdf['geometry'].x, ubs_gdf['geometry'].y)))

        # Locating the nearest nodes in the network.
        demand_nodes_array = network.get_node_ids(demand_coords[:, 0], demand_coords[:, 1])
        ubs_nodes_array = network.get_node_ids(ubs_coords[:, 0], ubs_coords[:, 1])

        invalid_demand_nodes = np.where(demand_nodes_array == -1)[0]
        invalid_ubs_nodes = np.where(ubs_nodes_array == -1)[0]

        # If we find points outside the network, increase the buffer and try again.
        if len(invalid_demand_nodes) > 0 or len(invalid_ubs_nodes) > 0:
            logger.warning(
                f"{len(invalid_demand_nodes)} demand points and "
                f"{len(invalid_ubs_nodes)} facilities outside the network."
            )
            buffer_size = min(buffer_size * 1.5, 1.0)
            logger.info(
                f"Increasing buffer to {buffer_size} degrees and rebuilding the network..."
            )
            attempt += 1
            continue
        else:
            logger.info("All points have been successfully mapped onto the network.")
            break

    # If we reach here after exhausting 'max_attempts', it has completely failed.
    if attempt == max_attempts and (graph is None or demand_nodes_array is None or ubs_nodes_array is None):
        raise RuntimeError(
            "Unable to obtain the network or map the points after several attempts."
        )

    # Remove invalid records if there are still nodes outside the network.
    if len(invalid_demand_nodes) > 0:
        demands_gdf = demands_gdf.drop(demands_gdf.index[invalid_demand_nodes]).reset_index(drop=True)
        demand_coords = np.delete(demand_coords, invalid_demand_nodes, axis=0)
        demand_nodes_array = np.delete(demand_nodes_array, invalid_demand_nodes)

    if len(invalid_ubs_nodes) > 0:
        ubs_gdf = ubs_gdf.drop(ubs_gdf.index[invalid_ubs_nodes]).reset_index(drop=True)
        ubs_coords = np.delete(ubs_coords, invalid_ubs_nodes, axis=0)
        ubs_nodes_array = np.delete(ubs_nodes_array, invalid_ubs_nodes)

    # Convert arrays to Series with the same index as the GeoDataFrame.
    demand_nodes = pd.Series(demand_nodes_array, index=demands_gdf.index)
    ubs_nodes = pd.Series(ubs_nodes_array, index=ubs_gdf.index)

    # Pre-calculate distances up to max_distance.
    network.precompute(max_distance)

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

    # Replace infinities with NaN.
    distances[np.isinf(distances)] = np.nan
    distance_df = pd.DataFrame(distances, index=demands_gdf.index, columns=ubs_gdf.index)

    # Infer the ID columns.
    col_demand_id = infer_column(demands_gdf, settings.DEMAND_ID_POSSIBLE_COLUMNS)
    col_name = infer_column(ubs_gdf, settings.NAME_POSSIBLE_COLUMNS)

    demands_ids = demands_gdf[col_demand_id].values
    ubs_names = ubs_gdf[col_name].values

    distance_df.index = demands_ids
    distance_df.columns = ubs_names

    return distance_df, network, graph, nodes, edges, demand_nodes, ubs_nodes
