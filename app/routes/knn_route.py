from fastapi import APIRouter, UploadFile, Query, HTTPException
from enum import Enum
from typing import Optional
from fastapi.responses import FileResponse, JSONResponse
import logging
import os
import uuid
import pandas as pd
import io
import json
import unicodedata

from app.preprocessing.common import prepare_data
from app.methods.knn_model import allocate_demands_knn

logger = logging.getLogger(__name__)
router = APIRouter()

# Step 1: Create the Enum for methods
class MethodEnum(str, Enum):
    pandana_real_distance = "pandana_real_distance"
    geodesic = "geodesic"

# Optional: Create a dropdown for output_format
class OutputFormatEnum(str, Enum):
    csv = "csv"
    geojson = "geojson"
    json = "json"

def normalize_str(s: str) -> str:
    return unicodedata.normalize("NFKD", s.strip().lower()).encode("ascii", "ignore").decode("utf-8")

@router.post("/allocate_demands_knn/")
def allocate_demands_knn_api(
    opportunities_file: UploadFile,
    demands_file: UploadFile,
    state: str = Query("", description="State (optional)"),
    city: str = Query("", description="City (optional)"),
    # Novo parâmetro: JSON array de cidades
    cities: str = Query("", description="Optional JSON array of cities for allocation (if provided, multi-city allocation)"),
    k: int = Query(1, description="Number of neighbors for KNN"),
    method: MethodEnum = Query(MethodEnum.pandana_real_distance, description="Choose the allocation method"),
    output_format: OutputFormatEnum = Query(OutputFormatEnum.csv, description="Choose the output format: 'csv', 'geojson', or 'json'")
):
    """
    Rota que permite alocar demandas usando KNN.
    - Se 'cities' (JSON array) for fornecido, faz alocação multi-cidade.
    - Caso contrário, usa apenas 'city'.
    """
    logger.info("Received request to allocate demands using KNN.")
    logger.info(
        "Parameters: state=%s, city=%s, cities=%s, k=%d, method=%s, output_format=%s",
        state, city, cities, k, method, output_format
    )

    # 1) Ler arquivos em memória (para evitar erro de formatação na segunda leitura)
    opportunities_content = opportunities_file.file.read()
    demands_content = demands_file.file.read()

    # 2) Tentar interpretar 'cities' como JSON array (se fornecido)
    cities_list = []
    if cities:
        try:
            cities_list = json.loads(cities)
            if not isinstance(cities_list, list):
                raise ValueError("Parameter 'cities' must be a JSON array.")
        except Exception as e:
            logger.exception("Error parsing 'cities' parameter.")
            raise HTTPException(status_code=400, detail="Invalid 'cities' parameter. Must be a JSON array string.")

    # Função auxiliar que encapsula a lógica de prepare_data,
    # mas lendo os bytes em memória ao invés de ler o arquivo repetidamente
    def prepare_data_from_bytes(
        opp_bytes: bytes,
        dem_bytes: bytes,
        state: str = "",
        city_filter: str = ""
    ):
        from app.preprocessing.common import prepare_data
        import geopandas as gpd

        # Criar UploadFiles “falsos” usando BytesIO
        opp_file = io.BytesIO(opp_bytes)
        dem_file = io.BytesIO(dem_bytes)

        class FakeUploadFile:
            def __init__(self, content):
                self.file = content

        fake_opp = FakeUploadFile(opp_file)
        fake_dem = FakeUploadFile(dem_file)

        # Agora chamar prepare_data normalmente
        return prepare_data(fake_opp, fake_dem, state=state, city=city_filter)

    results_df_list = []

    if cities_list:
        # Modo multi-cidade
        logger.info("Allocating demands for multiple cities: %s", cities_list)
        for city_name in cities_list:
            # Preparar GDF sem city (pois filtraremos manualmente ou passaremos None)
            error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city, col_state_opp, col_state_dem = prepare_data_from_bytes(
                opportunities_content,
                demands_content,
                state=state,
                city_filter=None  # Passa None para não filtrar aqui
            )
            if error:
                logger.error("Error in prepare_data: %s", error)
                raise HTTPException(status_code=400, detail=str(error))

            # Filtra manualmente demands_gdf e opportunities_gdf para city_name
            city_norm = normalize_str(city_name)

            demands_city = demands_gdf[
                demands_gdf["NM_MUN"].astype(str).apply(normalize_str) == city_norm
            ]
            opp_city = opportunities_gdf[
                opportunities_gdf[col_city].astype(str).apply(normalize_str) == city_norm
            ]
            if demands_city.empty or opp_city.empty:
                logger.warning("No records found for city '%s'. Skipping.", city_name)
                continue

            # Chama allocate_demands_knn
            logger.info("Allocating demands for city: %s", city_name)
            partial_df = allocate_demands_knn(
                demands_city,
                opp_city,
                col_demand_id,
                col_name,
                col_city,
                col_state_opp,
                k=k,
                method=method,
                city_name=city_name,
                num_threads=1
            )
            partial_df["city_allocated"] = city_name
            results_df_list.append(partial_df)

        if not results_df_list:
            raise HTTPException(status_code=404, detail="No records found for any provided city.")
        result_df = pd.concat(results_df_list, ignore_index=True)

    else:
        # Modo cidade única (ou sem city)
        logger.info("Allocating demands for single city='%s' or entire region if blank.", city)
        error, demands_gdf, opportunities_gdf, col_demand_id, col_name, col_city, col_state_opp, col_state_dem = prepare_data_from_bytes(
            opportunities_content,
            demands_content,
            state=state,
            city_filter=city  # city normal
        )
        if error:
            logger.error("Error in prepare_data: %s", error)
            return error

        result_df = allocate_demands_knn(
            demands_gdf,
            opportunities_gdf,
            col_demand_id,
            col_name,
            col_city,
            col_state_opp,
            k=k,
            method=method,
            city_name=city,
            num_threads=1
        )

    logger.info("Allocation completed successfully. Number of rows in result: %d", len(result_df))

    # 3) Salvar resultado conforme output_format
    file_id = str(uuid.uuid4())
    OUTPUT_DIR = "/tmp/api_output/"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info("Saving output to directory: %s", OUTPUT_DIR)

    if output_format == "csv":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.csv")
        result_df.to_csv(output_file, index=False)
        logger.info("Returning CSV file: %s", output_file)
        return FileResponse(output_file, media_type="text/csv", filename=os.path.basename(output_file))
    elif output_format == "geojson":
        output_file = os.path.join(OUTPUT_DIR, f"allocation_result_{file_id}.geojson")
        result_df.to_json(output_file, index=False, orient="records")
        logger.info("Returning GeoJSON file: %s", output_file)
        return FileResponse(output_file, media_type="application/geo+json", filename=os.path.basename(output_file))
    elif output_format == "json":
        logger.info("Returning JSON response directly.")
        return JSONResponse(content=result_df.to_dict(orient="records"))
    else:
        logger.error("Invalid output format requested: %s", output_format)
        raise HTTPException(status_code=400, detail="Invalid output format. Choose 'csv', 'geojson', or 'json'.")
