import geopandas as gpd
import matplotlib.pyplot as plt
from fastapi import HTTPException
from io import BytesIO


def plot_centroids(gdf):
    
    plt.figure(figsize=(20, 20))
    gdf.centroid.plot(marker='o', color='blue', markersize=5)
    
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")

    img_path = "centroids_plot.png"
    plt.savefig(img_path, format="png")
    plt.close()
    
    return img_path

async def validate_file_format(file, column_name=None):
    """
    Verifica se o arquivo está no formato correto (GeoDataFrame ou Shapefile),
    retorna os nomes das colunas do DataFrame, verifica se uma coluna específica existe e extrai os centroides.
    """
    try:
        content = await file.read()
        gdf = gpd.read_file(BytesIO(content))
        if gdf.empty:
            raise HTTPException(status_code=400, detail="O arquivo está vazio.")
        columns = list(gdf.columns)
        column_exists = column_name in columns if column_name else None
        centroids = []
        if 'geometry' in gdf.columns:
            centroids = gdf.geometry.centroid.apply(lambda geom: (geom.x, geom.y)).tolist()
            img_path = plot_centroids(gdf)
        else:
            raise HTTPException(status_code=400, detail="O arquivo não contém uma coluna de geometria.")
        return {
            "format": "GeoDataFrame/Shapefile",
            "columns": columns,
            "column_exists": column_exists,
            "centroids": centroids,
            "gdf": gdf,
            "plot": img_path
        }
    except Exception:
        raise HTTPException(status_code=400, detail="Arquivo inválido. Deve ser um Shapefile ou GeoJSON.")
