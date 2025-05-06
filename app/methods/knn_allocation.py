import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def select_knn_from_distance_matrix(
    distance_df: pd.DataFrame,
    k: int = 1
) -> pd.DataFrame:
    """
    Convert a wide distance_df into a long table with top-k neighbors per row.
    """
    logger.info("select_knn_from_distance_matrix: extracting top %d neighbors per demand.", k)
    results = []
    for demand_id, row in distance_df.iterrows():
        row_no_nan = row.dropna().sort_values()
        top_k = row_no_nan.iloc[:k]
        temp_df = pd.DataFrame({
            'demand_id': demand_id,
            'opportunity_name': top_k.index,
            'distance_km': top_k.values
        })
        results.append(temp_df)

    knn_df = pd.concat(results, ignore_index=True)
    logger.debug("select_knn_from_distance_matrix: final knn_df has %d rows.", len(knn_df))
    return knn_df


def join_knn_with_geometries(
    knn_df: pd.DataFrame,
    demands_gdf,
    opportunities_gdf,
    col_demand_id,
    col_name,
    col_city,      # coluna de cidade (oportunidades)
    col_state      # coluna de estado (oportunidades)
) -> pd.DataFrame:
    logger.info("join_knn_with_geometries: mapping lat/lon and additional attributes from demands and opportunities.")
    
    # Mapeamento para as coordenadas dos demands
    demands_map = {}
    for i, row in demands_gdf.iterrows():
        d_id = row[col_demand_id]
        demands_map[d_id] = (row.geometry.y, row.geometry.x)
    
    # Mapeamento para as coordenadas, cidade e estado das oportunidades
    opp_map = {}
    opp_city_map = {}
    opp_state_map = {}
    for i, row in opportunities_gdf.iterrows():
        opp_name = str(row[col_name])
        opp_map[opp_name] = (row.geometry.y, row.geometry.x)
        opp_city_map[opp_name] = row[col_city]    # valor da cidade
        opp_state_map[opp_name] = row[col_state]    # valor do estado

    lat_origin = []
    lon_origin = []
    lat_dest = []
    lon_dest = []
    destination_city = []
    destination_state = []
    
    for idx, row_item in knn_df.iterrows():
        d_id = row_item['demand_id']
        o_name = row_item['opportunity_name']

        if d_id in demands_map:
            lat_o, lon_o = demands_map[d_id]
        else:
            lat_o, lon_o = (None, None)

        if o_name in opp_map:
            lat_d, lon_d = opp_map[o_name]
            city_value = opp_city_map.get(o_name, None)
            state_value = opp_state_map.get(o_name, None)
        else:
            lat_d, lon_d = (None, None)
            city_value = None
            state_value = None

        lat_origin.append(lat_o)
        lon_origin.append(lon_o)
        lat_dest.append(lat_d)
        lon_dest.append(lon_d)
        destination_city.append(city_value)
        destination_state.append(state_value)

    knn_df['Origin_Lat'] = lat_origin
    knn_df['Origin_Lon'] = lon_origin
    knn_df['Destination_Lat'] = lat_dest
    knn_df['Destination_Lon'] = lon_dest
    knn_df['Destination_City'] = destination_city
    knn_df['Destination_State'] = destination_state

    logger.info("join_knn_with_geometries: appended geometry and additional attribute columns to knn DataFrame.")
    return knn_df
