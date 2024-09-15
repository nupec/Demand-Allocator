import geopandas as gpd
from app.geoprocessing.geoprocessing import process_geometries
from app.utils.utils import infer_column
from unidecode import unidecode
from app.config import settings

def prepare_data(establishments_file, demands_file, state, city=None):
    establishments_gdf = gpd.read_file(establishments_file.file)
    demands_gdf = gpd.read_file(demands_file.file)

    # Process centroids if necessary
    establishments_gdf = process_geometries(establishments_gdf)
    demands_gdf = process_geometries(demands_gdf)

    # Infer column names
    col_demand_id = infer_column(demands_gdf, settings.DEMAND_ID_POSSIBLE_COLUMNS)
    col_name = infer_column(establishments_gdf, settings.NAME_POSSIBLE_COLUMNS)
    col_city = infer_column(establishments_gdf, settings.CITY_POSSIBLE_COLUMNS)
    col_state_establishment = infer_column(establishments_gdf, settings.STATE_POSSIBLE_COLUMNS)
    col_state_demand = infer_column(demands_gdf, settings.STATE_POSSIBLE_COLUMNS)

    # Check if all necessary columns were inferred
    if not col_demand_id or not col_name or not col_city or not col_state_establishment or not col_state_demand:
        return {"error": "Could not infer all necessary columns. Please check the input data."}, None, None, None, None

    # Filter establishments by state and city
    establishments_gdf = establishments_gdf[establishments_gdf[col_state_establishment] == state]
    if city:
        city = unidecode(city.lower())
        establishments_gdf = establishments_gdf[establishments_gdf[col_city].apply(lambda x: unidecode(x.lower())) == city]

    # Filter demands by state and city
    demands_gdf = demands_gdf[demands_gdf[col_state_demand] == state]
    if city:
        demands_gdf = demands_gdf[demands_gdf['NM_MUN'].apply(lambda x: unidecode(x.lower())) == city]

    return None, demands_gdf, establishments_gdf, col_demand_id, col_name, col_city
