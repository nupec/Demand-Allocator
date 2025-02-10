from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from fastapi.responses import JSONResponse
from app.analysis.socioeconomic_analys import validate_file_format 
import json

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
            # Verifica se a coluna "municipio" existe
            if "MUNICÍPIO" in result["columns"]:
                # Filtra os dados com base no valor fornecido
                filtered_gdf = gdf[gdf["MUNICÍPIO"] == value]  

                # Verifica se há dados para o valor filtrado
                if filtered_gdf.empty:
                    raise HTTPException(status_code=404, detail=f"Nenhum dado encontrado para o município '{value}'.")

                # Extração dos centroides para os dados filtrados
                centroids_filtered = filtered_gdf.geometry.centroid.apply(lambda geom: (geom.x, geom.y)).tolist()
                
                # Adiciona os centroides filtrados ao retorno
                centroid_data = {"centroids": centroids_filtered}
            else:
                raise HTTPException(status_code=400, detail="A coluna 'municipio' não foi encontrada no arquivo.")
        else:
            # Caso não haja filtro, retorna todos os centroides
            centroid_data = {"centroids": result["centroids"]}

        # Retorna os centroides como um arquivo JSON
        return JSONResponse(
            content=centroid_data,
            media_type="application/json",
            headers={"Content-Disposition": "attachment; filename=centroids.json"}
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao processar o arquivo: {e}")
