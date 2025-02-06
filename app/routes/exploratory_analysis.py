from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from fastapi.responses import JSONResponse
from app.analysis.socioeconomic_analys import validate_file_format  # Importa a função de validação
import json

router = APIRouter()

@router.post("/api/validate-file")
async def validate_file(
    file: UploadFile = File(...),
    column_name: str = Query(None, description="Nome da coluna a ser validada (opcional)")
):
    """
    Verifica se o arquivo enviado está no formato correto (GeoDataFrame ou Shapefile),
    retorna os nomes das colunas do DataFrame, verifica se uma coluna específica existe e extrai os centroides.
    """
    try:
        # Valida o arquivo e extrai os centroides
        result = await validate_file_format(file, column_name)
        
        # Cria o dicionário de resposta com as informações gerais
        response = {
            "message": "Arquivo válido!",
            "format": result["format"],
            "columns": result["columns"],
        }

        # Adiciona os centroides ao arquivo JSON para retorno
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
