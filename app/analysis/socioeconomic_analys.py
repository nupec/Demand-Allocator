import geopandas as gpd
from fastapi import HTTPException
from io import BytesIO

async def validate_file_format(file, column_name=None):
    """
    Verifica se o arquivo está no formato correto (GeoDataFrame ou Shapefile),
    retorna os nomes das colunas do DataFrame e verifica se uma coluna específica existe.
    """
    try:
        content = await file.read()
        gdf = gpd.read_file(BytesIO(content))

        if gdf.empty:
            raise HTTPException(status_code=400, detail="O arquivo está vazio.")

        columns = list(gdf.columns)
        column_exists = column_name in columns if column_name else None

        return {
            "format": "GeoDataFrame/Shapefile",
            "columns": columns,
            "column_exists": column_exists
        }

    except Exception:
        raise HTTPException(status_code=400, detail="Arquivo inválido. Deve ser um Shapefile ou GeoJSON.")
