"""
Rota /consulta_base – alocação KNN + geração de artefatos (EDA) + configuração KeplerGL
------------------------------------------------------------------------------------
• Produz o **mesmo ZIP** da rota /api/eda/allocation, mas de forma automática, a partir
de um estado/município escolhidos na interface.
• Gera CSV e configuração de mapa sempre em  *frontend/data/temp*  e  *frontend/config/temp*  
  (não toca em nenhum arquivo permanente).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
import zipfile
from datetime import datetime
from io import BytesIO
from typing import Dict, Tuple

import geopandas as gpd
import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse, StreamingResponse

from app.analysis.socioeconomic_analys import analyze_knn_allocation
from app.config import settings
from app.lib.convert_numpy import convert_numpy_types
from app.methods.knn_model import allocate_demands_knn
from app.preprocessing.common import prepare_data
from app.routes.eda_allocation_route import (
    analyze_allocation,
    create_allocation_charts,
    create_coverage_stats,
    create_distance_boxplot,
    create_distance_hist,
    create_summary_table,
    generate_allocation_pdf,
    save_summary_table_image,
    gerar_perguntas_respostas,    
)
num_threads = os.cpu_count()
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/consulta_base")

# ------------------------------------------------------------------ #
# Diretórios e caminhos fixos
# ------------------------------------------------------------------ #
BACK_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FRONT_ROOT = os.path.abspath(os.path.join(BACK_ROOT, "..", "frontend"))

DEMANDS_PATH = os.path.join(BACK_ROOT, "data", "demands.geojson")
OPPORTUNITIES_PATH = os.path.join(BACK_ROOT, "data", "opportunities.geojson")

FRONT_DATA_DIR = os.path.join(FRONT_ROOT, "data", "temp")
FRONT_CONFIG_DIR = os.path.join(FRONT_ROOT, "config", "temp")

os.makedirs(FRONT_DATA_DIR, exist_ok=True)
os.makedirs(FRONT_CONFIG_DIR, exist_ok=True)

# ------------------------------------------------------------------ #
# Cache de ZIPs em memória (TTL simples)
# ------------------------------------------------------------------ #
ZIP_CACHE: Dict[str, Tuple[datetime, bytes]] = {}
ZIP_TTL_MIN = 30  # minutos


def _clean_zip_cache() -> None:
    """Remove ZIPs expirados do cache (TTL)."""
    from datetime import timedelta

    cutoff = datetime.utcnow() - timedelta(minutes=ZIP_TTL_MIN)
    for key, (ts, _) in list(ZIP_CACHE.items()):
        if ts < cutoff:
            del ZIP_CACHE[key]


# ------------------------------------------------------------------ #
# Utilidades KeplerGL – geração de configuração dinâmica
# ------------------------------------------------------------------ #

def _uid() -> str:
    return uuid.uuid4().hex[:7]


def build_kepler_config(
    csv_filename: str,
    center_lat: float,
    center_lon: float,
    zoom: float = 8.6,
    lat_o: str = "Origin_Lat",
    lon_o: str = "Origin_Lon",
    lat_d: str = "Destination_Lat",
    lon_d: str = "Destination_Lon",
) -> dict:
    """Retorna um dicionário pronto para ser salvo como JSON de configuração do KeplerGL."""

    origin_id, dest_id, arc_id, line_id = (_uid() for _ in range(4))

    # ------------------------------------------------------------------
    # 1) Paleta (gradiente azul → vermelho) usada nos arcos
    # ------------------------------------------------------------------
    TEMPERATURE_RANGE = {
        "name": "Temperature",
        "type": "sequential",
        "category": "Uber",
        "colors": [
            "#2b83ba",   # distâncias menores  (frio)
            "#4ea0c0",
            "#71b7c8",
            "#92cad0",
            "#b4dcd5",
            "#f4e0c0",
            "#fdb663",
            "#f57d4e",
            "#d7191c"    # distâncias maiores (quente)
        ],
        "reversed": False
    }

    # ------------------------------------------------------------------
    # 2) Função utilitária para camadas de ponto
    # ------------------------------------------------------------------
    def point_layer(layer_id, label, color, lat, lon, radius, opacity):
        return {
            "id": layer_id,
            "type": "point",
            "config": {
                "dataId": csv_filename,
                "label": label,
                "color": color,
                "highlightColor": [252, 242, 26, 255],
                "columns": {"lat": lat, "lng": lon, "altitude": None},
                "isVisible": True,
                "visConfig": {
                    "radius": radius,
                    "fixedRadius": False,
                    "opacity": opacity,
                    "outline": False,
                    "thickness": 2,
                    "strokeColor": None,
                    "radiusRange": [0, 50],
                    "filled": True,
                },
                "hidden": False,
                "textLabel": [
                    {
                        "field": None,
                        "color": [255, 255, 255],
                        "size": 18,
                        "offset": [0, 0],
                        "anchor": "start",
                        "alignment": "center",
                    }
                ],
            },
            "visualChannels": {
                "colorField": None,
                "colorScale": "quantile",
                "strokeColorField": None,
                "strokeColorScale": "quantile",
                "sizeField": None,
                "sizeScale": "linear",
            },
        }

    # ------------------------------------------------------------------
    # 3) Camadas
    # ------------------------------------------------------------------
    origin_layer = point_layer(
        origin_id, "origin",
        [117, 222, 227],      # azul-turquesa
        lat_o, lon_o, 10, 0.31
    )

    destination_layer = point_layer(
        dest_id, "destination",
        [227, 26, 26],        # vermelho (fallback)
        lat_d, lon_d, 15.4, 0.8
    )

    arc_layer = {
        "id": arc_id,
        "type": "arc",
        "config": {
            "dataId": csv_filename,
            "label": "origin → destination arc",
            "color": [100, 100, 100],            # cor neutra de fallback
            "highlightColor": [255, 255, 255],
            "columns": {
                "lat0": lat_o, "lng0": lon_o,
                "lat1": lat_d, "lng1": lon_d
            },
            "isVisible": True,
            "visConfig": {
                "opacity": 0.8,
                "thickness": 2,
                "sizeRange": [0, 10],
                "colorRange": TEMPERATURE_RANGE      # uso do gradiente
            },
            "hidden": False,
            "textLabel": []
        },
        "visualChannels": {
            "colorField": {"name": "distance_km", "type": "real"},
            "colorScale": "quantile"               # “continuous” 
        }
    }

    line_layer = {
        "id": line_id,
        "type": "line",
        "config": {
            "dataId": csv_filename,
            "label": "origin → destination line",
            "color": [130, 154, 227],
            "highlightColor": [252, 242, 26, 255],
            "columns": {
                "lat0": lat_o, "lng0": lon_o, "alt0": None,
                "lat1": lat_d, "lng1": lon_d, "alt1": None
            },
            "isVisible": False,
            "visConfig": {"opacity": 0.8, "thickness": 2},
            "hidden": False,
            "textLabel": []
        }
    }

    # ------------------------------------------------------------------
    # 4) Configuração final retornada ao KeplerGL
    # ------------------------------------------------------------------
    return {
        "version": "v1",
        "config": {
            "visState": {
                "filters": [],
                "layers": [origin_layer, destination_layer, arc_layer, line_layer],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            csv_filename: [
                                {"name": "demand_id"},
                                {"name": "Destination_State"},
                                {"name": "Destination_City"},
                                {"name": "opportunity_name"},
                                {"name": "Origin_Lat"},
                            ]
                        },
                        "enabled": True,
                    }
                },
                "layerBlending": "normal",
                "splitMaps": [],
            },
            "mapState": {
                "bearing": -33,
                "dragRotate": True,
                "latitude": round(center_lat, 6),
                "longitude": round(center_lon, 6),
                "pitch": 59,
                "zoom": zoom,
                "isSplit": False,
            },
            "mapStyle": {"styleType": "dark"},
        },
    }

# ------------------------------------------------------------------ #
# Pré-carrega lista de UFs/municípios (para selects no frontend)
# ------------------------------------------------------------------ #
try:
    DEMANDS_GDF = gpd.read_file(DEMANDS_PATH)
except Exception as e:
    logger.error("Erro ao carregar %s: %s", DEMANDS_PATH, e)
    DEMANDS_GDF = gpd.GeoDataFrame()


@router.get("/ufs")
def get_ufs():
    if DEMANDS_GDF.empty:
        return JSONResponse(status_code=500, content={"error": "demands.geojson não carregado"})
    return sorted(DEMANDS_GDF["UF"].dropna().unique().tolist())


@router.get("/municipios")
def get_municipios(uf: str = Query("")):
    if DEMANDS_GDF.empty:
        return JSONResponse(status_code=500, content={"error": "demands.geojson não carregado"})
    cidades = DEMANDS_GDF.loc[DEMANDS_GDF["UF"] == uf, "NM_MUN"] if uf else DEMANDS_GDF["NM_MUN"]
    return sorted(cidades.dropna().unique().tolist())


# ------------------------------------------------------------------ #
# Limpa diretórios temporários ao subir o servidor
# ------------------------------------------------------------------ #

def _limpar_temp():
    shutil.rmtree(FRONT_DATA_DIR, ignore_errors=True)
    shutil.rmtree(FRONT_CONFIG_DIR, ignore_errors=True)
    os.makedirs(FRONT_DATA_DIR, exist_ok=True)
    os.makedirs(FRONT_CONFIG_DIR, exist_ok=True)
    with open(os.path.join(FRONT_CONFIG_DIR, "config.json"), "w", encoding="utf-8") as f:
        json.dump({"siteTitle": "VisKepler", "websiteTitle": "My SisKepler App", "maps": []}, f)


_limpar_temp()

# ------------------------------------------------------------------ #
# Rota principal – produz dashboard + ZIP + mapa
# ------------------------------------------------------------------ #

@router.get("/resultado_completo")
def consulta_completa(uf: str = Query(...), municipio: str = Query(...), tipo: str = Query("geodesic")):
    try:
        logger.info("Consulta UF=%s | Mun=%s | Tipo=%s", uf, municipio, tipo)

        # ------------------------------------------------------------------
        # 1) Lê dados origem/destino filtrando Estado + Município
        # ------------------------------------------------------------------
        class _Buf:
            def __init__(self, f):
                self.file = f

        with open(DEMANDS_PATH, "rb") as dem_f, open(OPPORTUNITIES_PATH, "rb") as opp_f:
            err, demands_gdf, opps_gdf, col_did, col_name, col_city, col_state_opp, _ = prepare_data(
                _Buf(opp_f), _Buf(dem_f), state=uf, city=municipio
            )
        if err:
            return JSONResponse(status_code=500, content={"erro": err})
        if demands_gdf.empty or opps_gdf.empty:
            return JSONResponse(status_code=404, content={"erro": "Sem dados suficientes"})

        # ------------------------------------------------------------------
        # 2) Executa o KNN e estatísticas de alta-nível (city_summary)
        # ------------------------------------------------------------------
        df_knn = allocate_demands_knn(
            demands_gdf,
            opps_gdf,
            col_did,
            col_name,
            col_city,
            col_state_opp,
            k=1,
            method=tipo,
            city_name=municipio,
            num_threads=num_threads
        )
        allocation_dict, city_summary = analyze_knn_allocation(df_knn, demands_gdf, opps_gdf, settings=settings)

        # ------------------------------------------------------------------
        # 3) Usa a rota EDA interna para gerar artefatos (UBS summary & PDF)
        # ------------------------------------------------------------------
        merged_df, summary_ubs = analyze_allocation(df_knn, demands_gdf)
        perguntas_guiadas = gerar_perguntas_respostas(summary_ubs, merged_df)
        # Gráficos, histograma, boxplot, tabela, PDF
        chart_pop_buf, chart_racial_buf = create_allocation_charts(summary_ubs)
        coverage_stats_df = create_coverage_stats(merged_df)
        hist_buf = create_distance_hist(merged_df)
        boxplot_buf = create_distance_boxplot(merged_df)
        resumo_df = create_summary_table(summary_ubs)
        resumo_img_buf = save_summary_table_image(resumo_df)
        pdf_buf = generate_allocation_pdf(summary_ubs, merged_df)

        # ------------------------------------------------------------------
        # 4) Monta ZIP completo em memória e salva no cache
        # ------------------------------------------------------------------
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("allocation_result.csv", df_knn.to_csv(index=False))
            z.writestr("allocation_merged.json", merged_df.to_json(orient="records", force_ascii=False))
            z.writestr("city_summary.json", json.dumps(convert_numpy_types(city_summary)))
            z.writestr("ubs_summary.csv", summary_ubs.to_csv(index=False))
            z.writestr("chart_population.png", chart_pop_buf.getvalue())
            z.writestr("chart_racial.png", chart_racial_buf.getvalue())
            z.writestr("table_indicadores.png", resumo_img_buf.getvalue())
            z.writestr("report.pdf", pdf_buf.getvalue())
            if not coverage_stats_df.empty:
                z.writestr("coverage_stats.csv", coverage_stats_df.to_csv(index=False))
            if hist_buf:
                z.writestr("distance_hist.png", hist_buf.getvalue())
            if boxplot_buf:
                z.writestr("distance_boxplot.png", boxplot_buf.getvalue())
        zip_buffer.seek(0)

        cache_key = f"{uf}_{municipio}_{tipo}"
        _clean_zip_cache()
        ZIP_CACHE[cache_key] = (datetime.utcnow(), zip_buffer.getvalue())

        # ------------------------------------------------------------------
        # 5) Gera arquivo CSV + configuração Kepler em diretórios temp
        # ------------------------------------------------------------------
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        map_id = f"knn_{timestamp}"
        csv_file = f"{map_id}.csv"
        cfg_file = f"{map_id}.json"

        df_knn.to_csv(os.path.join(FRONT_DATA_DIR, csv_file), index=False)

        # Centro aproximado para o mapa (usa centroide das demands)
        try:
            centroide = demands_gdf.geometry.unary_union.centroid
            center_lat, center_lon = centroide.y, centroide.x
        except Exception:
            center_lat = demands_gdf["Origin_Lat"].mean()
            center_lon = demands_gdf["Origin_Lon"].mean()

        kepler_cfg = build_kepler_config(csv_file, center_lat, center_lon)
        kepler_cfg["label"] = f"Alocação – {municipio}/{uf}"
        with open(os.path.join(FRONT_CONFIG_DIR, cfg_file), "w", encoding="utf-8") as f:
            json.dump(kepler_cfg, f, ensure_ascii=False, indent=2)

        # Atualiza config.json TEMP para o frontend listar o mapa
        config_temp_path = os.path.join(FRONT_CONFIG_DIR, "config.json")
        try:
            with open(config_temp_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {"siteTitle": "VisKepler", "websiteTitle": "My SisKepler App", "maps": []}

        entry = {
            "data_ids": {"csv_file": csv_file},
            "label": f"Alocação – {municipio}/{uf}",
            "link": f"/map/{map_id}",
            "description": f"Fluxo Demanda → UBS ({tipo}) gerado em {timestamp}",
        }
        if not any(m["link"] == entry["link"] for m in cfg["maps"]):
            cfg["maps"].append(entry)
        with open(config_temp_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

        # ------------------------------------------------------------------
        # 6) Resposta JSON (para o dashboard)
        # ------------------------------------------------------------------
        return {
            "alocacao": convert_numpy_types(allocation_dict),
            "eda": convert_numpy_types(city_summary),  
            "info": {"UF": uf, "Município": municipio, "Distância": tipo},
            "map_url": f"/map/{map_id}",
            "perguntas": perguntas_guiadas,
        }

    except Exception as e:
        logger.exception("Erro inesperado na rota /consulta_base/resultado_completo")
        return JSONResponse(status_code=500, content={"erro": str(e)})

# ------------------------------------------------------------------ #
# Download do ZIP em cache
# ------------------------------------------------------------------ #

@router.get("/download_zip")
def download_zip(uf: str = Query(...), municipio: str = Query(...), tipo: str = Query("geodesic")):
    cache_key = f"{uf}_{municipio}_{tipo}"
    cached = ZIP_CACHE.get(cache_key)
    if not cached:
        return JSONResponse(status_code=404, content={"erro": "ZIP não disponível. Execute a consulta primeiro."})

    _clean_zip_cache()
    _, zip_bytes = cached
    headers = {"Content-Disposition": f"attachment; filename=alocacao_{cache_key}.zip"}
    return StreamingResponse(BytesIO(zip_bytes), media_type="application/zip", headers=headers)