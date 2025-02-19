import pandas as pd
import numpy as np

def select_knn_from_distance_matrix(
    distance_df: pd.DataFrame,
    k: int = 1
) -> pd.DataFrame:
    """
    Receives a DataFrame 'distance_df' where:
        - index = IDs (demands)
        - columns = names (opportunities)
        - values = distance
    Returns a long-form DataFrame with the K nearest opportunities for each demand,
    sorted by distance.
    
    Returns columns, for example:
      demand_id | opportunity_name | distance (km) | rank
    with rank = 1 for the closest, 2 for the second closest, etc.
    """
    # For each row (demand), sort the columns by distance
    results = []
    for demand_id, row in distance_df.iterrows():
        # Drop NaN values to avoid issues during sorting
        row_no_nan = row.dropna().sort_values()
        # Get the top k closest opportunities
        top_k = row_no_nan.iloc[:k]
        # Create a temporary DataFrame for each demand
        temp_df = pd.DataFrame({
            'demand_id': demand_id,
            'opportunity_name': top_k.index,
            'distance_km': top_k.values
        })
        # Create a rank column (1 to k)
        temp_df['rank'] = range(1, len(top_k) + 1)
        results.append(temp_df)

    # Concatenate all temporary DataFrames
    knn_df = pd.concat(results, ignore_index=True)
    return knn_df


def join_knn_with_geometries(
    knn_df: pd.DataFrame,
    demands_gdf,
    opportunities_gdf,
    col_demand_id,
    col_name
) -> pd.DataFrame:
    """
    Joins the knn_df (which contains demand_id and opportunity_name)
    to add columns with the latitude/longitude for origin and destination, etc.
    """
    # Map demand_id -> (lat, lon)
    demands_map = {}
    for i, row in demands_gdf.iterrows():
        d_id = row[col_demand_id]
        demands_map[d_id] = (row.geometry.y, row.geometry.x)

    # Map opportunity_name -> (lat, lon)
    opp_map = {}
    for i, row in opportunities_gdf.iterrows():
        opp_name = str(row[col_name])  # convert to string
        opp_map[opp_name] = (row.geometry.y, row.geometry.x)

    # Iterate through knn_df and create the lat/lon columns
    lat_origin = []
    lon_origin = []
    lat_dest = []
    lon_dest = []

    for idx, row in knn_df.iterrows():
        d_id = row['demand_id']
        o_name = row['opportunity_name']

        if d_id in demands_map:
            lat_o, lon_o = demands_map[d_id]
        else:
            lat_o, lon_o = (np.nan, np.nan)

        if o_name in opp_map:
            lat_d, lon_d = opp_map[o_name]
        else:
            lat_d, lon_d = (np.nan, np.nan)

        lat_origin.append(lat_o)
        lon_origin.append(lon_o)
        lat_dest.append(lat_d)
        lon_dest.append(lon_d)

    knn_df['Origin_Lat'] = lat_origin
    knn_df['Origin_Lon'] = lon_origin
    knn_df['Destination_Lat'] = lat_dest
    knn_df['Destination_Lon'] = lon_dest

    return knn_df
