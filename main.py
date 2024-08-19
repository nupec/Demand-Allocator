import geopandas as gpd 
from geopy.distance import geodesic  
import pandas as pd  
from fastapi import FastAPI, Query  
from typing import Optional  
from unidecode import unidecode  

app = FastAPI()

# Função para calcular a distância geodésica entre dois pontos (em quilômetros)
def calcular_distancia_geodesica(ponto1, ponto2):
    return geodesic(ponto1, ponto2).kilometers

# Função para alocar demandas (setores censitários) para a UBS mais próxima
def alocar_demandas(demandas_gdf, ubs_gdf):
    alocacao = []  

    for i, demanda in enumerate(demandas_gdf.itertuples(), 1):
        ponto_demanda = (demanda.geometry.y, demanda.geometry.x) 
        menor_distancia = float('inf') 
        ubs_mais_proxima = None  

        print(f"Processando demanda {i}/{len(demandas_gdf)} - ID Setor: {demanda.CD_SETOR}")
        print(f"Coordenadas da Demanda: {ponto_demanda}")

        # Loop para comparar cada demanda com todas as UBSs
        for j, ubs in enumerate(ubs_gdf.itertuples(), 1):
            ponto_ubs = (ubs.geometry.y, ubs.geometry.x) 
            distancia = calcular_distancia_geodesica(ponto_demanda, ponto_ubs)  
            print(f"  Comparando com UBS {j}/{len(ubs_gdf)} - {ubs.NOME_UBS} em {ubs.MUNICÍPIO}: Distância = {distancia:.2f} km")

            # Se a distância atual for menor que a menor já encontrada, atualiza as variáveis
            if distancia < menor_distancia:
                menor_distancia = distancia
                ubs_mais_proxima = ubs

        # Verifica se encontrou uma UBS mais próxima e armazena as informações
        if ubs_mais_proxima:
            ubs_mais_proxima_dict = ubs_mais_proxima._asdict()  
            nome_ubs = ubs_mais_proxima_dict.get('NOME_UBS', None) 
            municipio_ubs = ubs_mais_proxima_dict.get('MUNICÍPIO', None) 
            print(f"  -> UBS mais próxima: {nome_ubs} em {municipio_ubs} com distância {menor_distancia:.2f} km")
        else:
            nome_ubs = None
            municipio_ubs = None
            print("  -> Nenhuma UBS encontrada")

        alocacao.append({
            'ID_Setor': demanda.CD_SETOR,  
            'UBS': nome_ubs,  
            'Município_UBS': municipio_ubs,  
            'Distância': menor_distancia 
        })

    return pd.DataFrame(alocacao)  

# Endpoint da API para alocar demandas com base na UF e opcionalmente no município
@app.get("/alocar_demandas/")
def alocar_demandas_api(uf: str, municipio: Optional[str] = None):
    print(f"Carregando UBSs do estado {uf}...")
    ubs_gdf = gpd.read_file('UBS_BRASIL.geojson')
    demandas_gdf = gpd.read_file('BR_Centroides_2022.geojson')

    # Verifica se as coordenadas estão no sistema EPSG:4326 
    if ubs_gdf.crs.to_string() != 'EPSG:4326':
        print("Convertendo coordenadas das UBSs para EPSG:4326...")
        ubs_gdf = ubs_gdf.to_crs(epsg=4326)

    if demandas_gdf.crs.to_string() != 'EPSG:4326':
        print("Convertendo coordenadas das demandas para EPSG:4326...")
        demandas_gdf = demandas_gdf.to_crs(epsg=4326)

    # Filtrar as UBSs pela UF especificada
    print(f"Filtrando UBSs pela UF {uf}...")
    ubs_gdf = ubs_gdf[ubs_gdf['UF'] == uf]
    
    # Filtrar as UBSs pelo município, se fornecido
    if municipio:
        print(f"Filtrando UBSs pelo município {municipio}...")
        municipio = unidecode(municipio.lower())  
        ubs_gdf = ubs_gdf[ubs_gdf['MUNICÍPIO'].apply(lambda x: unidecode(x.lower())) == municipio]

    # Filtrar as demandas pela UF especificada
    print(f"Filtrando demandas pela UF {uf}...")
    demandas_gdf = demandas_gdf[demandas_gdf['UF'] == uf]
    
    # Filtrar as demandas pelo município, se fornecido
    if municipio:
        print(f"Filtrando demandas pelo município {municipio}...")
        demandas_gdf = demandas_gdf[demandas_gdf['NM_MUN'].apply(lambda x: unidecode(x.lower())) == municipio]

    # Realizar a alocação das demandas
    print("Iniciando a alocação das demandas...")
    resultado_df = alocar_demandas(demandas_gdf, ubs_gdf)

    return resultado_df.to_dict(orient='records')

if __name__ == "__main__":
    import uvicorn  
    uvicorn.run(app, host="0.0.0.0", port=8000) 
