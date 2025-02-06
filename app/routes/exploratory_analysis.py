from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from app.analysis.socioeconomic_analys import validate_file_format

router = APIRouter()

@router.post("/api/validate-file")
async def validate_file(
    file: UploadFile = File(...),
    column_name: str = Query(None, description="Nome da coluna a ser validada (opcional)")
):
    """
    Verifica se o arquivo enviado está no formato correto (GeoDataFrame ou Shapefile),
    retorna os nomes das colunas do DataFrame e verifica se uma coluna específica existe.
    """
    try:
        result = await validate_file_format(file, column_name)
        response = {
            "message": "Arquivo válido!",
            "format": result["format"],
            "columns": result["columns"]
        }

        if column_name:
            response["column_exists"] = result["column_exists"]
            response["column_checked"] = column_name
            if not result["column_exists"]:
                raise HTTPException(status_code=400, detail=f"A coluna '{column_name}' não existe no arquivo.")

        return response

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao processar o arquivo: {e}")