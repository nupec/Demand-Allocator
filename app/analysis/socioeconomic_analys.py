import geopandas as gpd
from io import BytesIO
from fastapi import HTTPException

async def validate_file_format(file, column_name=None):
    """
    Verifica se o arquivo está no formato correto (GeoDataFrame ou Shapefile),
    retorna os nomes das colunas do DataFrame, verifica se uma coluna específica existe e extrai os centroides.
    """
    try:
        # Lê o conteúdo do arquivo enviado
        content = await file.read()
        gdf = gpd.read_file(BytesIO(content))  # Converte o arquivo em um GeoDataFrame

        # Verifica se o arquivo está vazio
        if gdf.empty:
            raise HTTPException(status_code=400, detail="O arquivo está vazio.")

        # Obtém as colunas do GeoDataFrame
        columns = list(gdf.columns)

        # Verifica se a coluna fornecida (se houver) existe no DataFrame
        column_exists = column_name in columns if column_name else None

        # Extração dos centroides (caso a coluna 'geometry' exista)
        centroids = []
        if 'geometry' in gdf.columns:
            centroids = gdf.geometry.centroid.apply(lambda geom: (geom.x, geom.y)).tolist()
        else:
            raise HTTPException(status_code=400, detail="O arquivo não contém uma coluna de geometria.")

        # Retorna as informações obtidas
        return {
            "format": "GeoDataFrame/Shapefile",  # O formato do arquivo
            "columns": columns,  # Colunas do GeoDataFrame
            "column_exists": column_exists,  # Verifica se a coluna existe
            "centroids": centroids  # Lista de centroides (coordenadas x, y)
        }

    except Exception:
        raise HTTPException(status_code=400, detail="Arquivo inválido. Deve ser um Shapefile ou GeoJSON.")
