import geopandas as gpd
from geopy.distance import geodesic
import pandas as pd
from fastapi import FastAPI, Query, UploadFile, File
from typing import Optional
from unidecode import unidecode

app = FastAPI(
    title="UBS-Demand-Allocator",
    description="API para alocação de demandas para UBSs com base em dados geográficos utilizando cálculos de distância geodésica.",
    version="1.0.0"
)

def inferir_coluna(gdf, possiveis_nomes):
    for nome in possiveis_nomes:
        colunas = [col for col in gdf.columns if unidecode(col).lower() == unidecode(nome).lower()]
        if colunas:
            return colunas[0]
    return None

def calcular_distancia_geodesica(ponto1, ponto2):
    return geodesic(ponto1, ponto2).kilometers

def alocar_demandas(demandas_gdf, ubs_gdf, col_demanda_id, col_ubs_nome, col_ubs_municipio):
    alocacao = []

    for i, demanda in enumerate(demandas_gdf.itertuples(), 1):
        ponto_demanda = (demanda.geometry.y, demanda.geometry.x)
        menor_distancia = float('inf')
        ubs_mais_proxima = None

        for ubs in ubs_gdf.itertuples():
            ponto_ubs = (ubs.geometry.y, ubs.geometry.x)
            distancia = calcular_distancia_geodesica(ponto_demanda, ponto_ubs)
            if distancia < menor_distancia:
                menor_distancia = distancia
                ubs_mais_proxima = ubs

        alocacao.append({
            'ID_Setor': getattr(demanda, col_demanda_id),
            'UBS': getattr(ubs_mais_proxima, col_ubs_nome) if ubs_mais_proxima else None,
            'Município_UBS': getattr(ubs_mais_proxima, col_ubs_municipio) if ubs_mais_proxima else None,
            'Distância': menor_distancia
        })

    return pd.DataFrame(alocacao)

@app.post("/alocar_demandas/")
def alocar_demandas_api(
    ubs_file: UploadFile = File(...),
    demandas_file: UploadFile = File(...),
    uf: str = Query(...),
    municipio: Optional[str] = None
):
    ubs_gdf = gpd.read_file(ubs_file.file)
    demandas_gdf = gpd.read_file(demandas_file.file)

    # Verificar se as geometrias são polígonos e calcular os centróides se necessário
    if ubs_gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon']).any():
        print("Calculando centróides para as UBSs...")
        ubs_gdf = ubs_gdf.to_crs(epsg=3857)  # Usar um CRS projetado para o cálculo dos centróides
        ubs_gdf['geometry'] = ubs_gdf.centroid
        ubs_gdf = ubs_gdf.to_crs(epsg=4326)  # Voltar para WGS84

    if demandas_gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon']).any():
        print("Calculando centróides para as demandas...")
        demandas_gdf = demandas_gdf.to_crs(epsg=3857)  # Usar um CRS projetado para o cálculo dos centróides
        demandas_gdf['geometry'] = demandas_gdf.centroid
        demandas_gdf = demandas_gdf.to_crs(epsg=4326)  # Voltar para WGS84

    # Inferir os nomes das colunas relevantes para UBS e demandas
    col_demanda_id = inferir_coluna(demandas_gdf, ['CD_SETOR', 'ID', 'SETOR'])
    col_ubs_nome = inferir_coluna(ubs_gdf, ['NOME_UBS', 'NOME', 'NAME', 'UBS'])
    col_ubs_municipio = inferir_coluna(ubs_gdf, ['MUNICÍPIO', 'MUNICIPIO', 'CIDADE', 'MUNICIPALITY', 'CITY', 'BOROUGH', 'COMMUNE', 'GEMEINDE', 'COMUNE'])
    col_ubs_uf = inferir_coluna(ubs_gdf, ['NM_UF','UF', 'ST', 'State', 'Province', 'Territory', 'BL', 'Provincia', 'Prov', 'EDO', 'Estado', 'Canton', 'CT', 'Région', 'Dept', 'Regione', 'County', 'CNTY', 'UT', 'Oblast', 'OBL', 'Distrito', 'DSTR'])
    col_demanda_uf = inferir_coluna(demandas_gdf, ['NM_UF','UF', 'ST', 'State', 'Province', 'Territory', 'BL', 'Provincia', 'Prov', 'EDO', 'Estado', 'Canton', 'CT', 'Région', 'Dept', 'Regione', 'County', 'CNTY', 'UT', 'Oblast', 'OBL', 'Distrito', 'DSTR'])

    print(f"Coluna de UF na UBS: {col_ubs_uf}, Coluna de UF na Demanda: {col_demanda_uf}")

    if not col_demanda_id or not col_ubs_nome or not col_ubs_municipio or not col_ubs_uf or not col_demanda_uf:
        return {"error": "Não foi possível inferir todas as colunas necessárias. Verifique os dados de entrada."}

    # Filtrar as UBSs pela UF
    ubs_gdf = ubs_gdf[ubs_gdf[col_ubs_uf] == uf]
    print(f"Número de UBSs após filtragem por UF '{uf}': {len(ubs_gdf)}")

    # Filtrar as UBSs pelo município, se aplicável
    if municipio:
        municipio = unidecode(municipio.lower())
        ubs_gdf = ubs_gdf[ubs_gdf[col_ubs_municipio].apply(lambda x: unidecode(x.lower())) == municipio]
        print(f"Número de UBSs após filtragem por Município '{municipio}': {len(ubs_gdf)}")

    # Filtrar as demandas pela UF
    demandas_gdf = demandas_gdf[demandas_gdf[col_demanda_uf] == uf]
    print(f"Número de demandas após filtragem por UF '{uf}': {len(demandas_gdf)}")

    # Filtrar as demandas pelo município, se aplicável
    if municipio:
        demandas_gdf = demandas_gdf[demandas_gdf['NM_MUN'].apply(lambda x: unidecode(x.lower())) == municipio]
        print(f"Número de demandas após filtragem por Município '{municipio}': {len(demandas_gdf)}")

    # Realizar a alocação das demandas
    resultado_df = alocar_demandas(demandas_gdf, ubs_gdf, col_demanda_id, col_ubs_nome, col_ubs_municipio)

    # Converter o resultado para JSON
    return resultado_df.to_dict(orient='records')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
