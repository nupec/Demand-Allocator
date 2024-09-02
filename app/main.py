from fastapi import FastAPI, Query, UploadFile, File
from typing import Optional
import geopandas as gpd
from unidecode import unidecode

from app.config import settings
from app.allocation.allocation import allocate_demands
from app.geoprocessing.geoprocessing import process_geometries
from app.utils.utils import infer_column, calculate_geodesic_distance

app = FastAPI(
    title=settings.APP_TITLE,
    description=settings.APP_DESCRIPTION,
    version=settings.APP_VERSION,
)

@app.post("/allocate_demands/")
def allocate_demands_api(
    establishments_file: UploadFile = File(...),
    demands_file: UploadFile = File(...),
    state: str = Query(...),
    city: Optional[str] = None
):
    establishments_gdf = gpd.read_file(establishments_file.file)
    demands_gdf = gpd.read_file(demands_file.file)

    # Calculate centroids if necessary
    establishments_gdf = process_geometries(establishments_gdf)
    demands_gdf = process_geometries(demands_gdf)

    # Infer column names
    col_demand_id = infer_column(demands_gdf, settings.DEMAND_ID_POSSIBLE_COLUMNS)
    col_name = infer_column(establishments_gdf, settings.NAME_POSSIBLE_COLUMNS)
    col_city = infer_column(establishments_gdf, settings.CITY_POSSIBLE_COLUMNS)
    col_state_establishment = infer_column(establishments_gdf, settings.STATE_POSSIBLE_COLUMNS)
    col_state_demand = infer_column(demands_gdf, settings.STATE_POSSIBLE_COLUMNS)

    if not col_demand_id or not col_name or not col_city or not col_state_establishment or not col_state_demand:
        return {"error": "Could not infer all necessary columns. Please check the input data."}

    # Filter establishments by state
    establishments_gdf = establishments_gdf[establishments_gdf[col_state_establishment] == state]
    print(f"Number of establishments after filtering by state '{state}': {len(establishments_gdf)}")

    # Filter establishments by city, if applicable
    if city:
        city = unidecode(city.lower())
        establishments_gdf = establishments_gdf[establishments_gdf[col_city].apply(lambda x: unidecode(x.lower())) == city]
        print(f"Number of establishments after filtering by city '{city}': {len(establishments_gdf)}")

    # Filter demands by state
    demands_gdf = demands_gdf[demands_gdf[col_state_demand] == state]
    print(f"Number of demands after filtering by state '{state}': {len(demands_gdf)}")

    # Filter demands by city, if applicable
    if city:
        demands_gdf = demands_gdf[demands_gdf['NM_MUN'].apply(lambda x: unidecode(x.lower())) == city]
        print(f"Number of demands after filtering by city '{city}': {len(demands_gdf)}")

    # Perform demand allocation
    result_df = allocate_demands(demands_gdf, establishments_gdf, col_demand_id, col_name, col_city)

    # Return the result
    return result_df.to_dict(orient='records')
