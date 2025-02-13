from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from fastapi.responses import StreamingResponse
from app.analysis.socioeconomic_analys import validate_file_format, plot_centroids
import json
import os
from io import BytesIO
import zipfile

router = APIRouter()

@router.post("/api/validate-file")
async def validate_file(
    file: UploadFile = File(...),
    column_name: str = Query(None, description="Nome da coluna a ser validada (opcional)"),
    value: str = Query(None, description="Valor para filtrar o dado no arquivo")  
):
    """
    Verifica se o arquivo enviado está no formato correto (GeoDataFrame ou Shapefile),
    retorna os nomes das colunas do DataFrame, verifica se uma coluna específica existe e extrai os centroides.
    Se um valor for fornecido, filtra os centroides com base nesse valor.
    """
    try:
        # Valida o arquivo e extrai os centroides
        result = await validate_file_format(file, column_name)
        gdf = result["gdf"]  

        # Se o valor para filtrar for fornecido, realiza a filtragem
        if value:
            if "MUNICÍPIO" in result["columns"]:
                filtered_gdf = gdf[gdf["MUNICÍPIO"] == value]  
                if filtered_gdf.empty:
                    raise HTTPException(status_code=404, detail=f"Nenhum dado encontrado para o município '{value}'.")
                # Filtra os centroides apenas para o município
                centroids_filtered = filtered_gdf.geometry.centroid.apply(lambda geom: (geom.x, geom.y)).tolist()
                centroid_data = {"centroids": centroids_filtered}

                # Plota os centroides apenas do município filtrado
                plot_path = plot_centroids(filtered_gdf)
            else:
                raise HTTPException(status_code=400, detail="A coluna 'MUNICÍPIO' não foi encontrada no arquivo.")
        else:
            centroid_data = {"centroids": result["centroids"]}
            plot_path = result.get("plot")

        # Salva os centroides em um arquivo JSON
        json_path = "centroids.json"
        with open(json_path, "w") as json_file:
            json.dump(centroid_data, json_file)

        # Gera o gráfico e obtém o caminho da imagem
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(json_path, "centroids.json")
            zipf.write(plot_path, "centroids_plot.png")
        zip_buffer.seek(0)

        return StreamingResponse(zip_buffer, media_type="application/zip", headers={"Content-Disposition": "attachment; filename=centroids_files.zip"})

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao processar o arquivo: {e}")
