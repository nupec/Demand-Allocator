from fastapi import APIRouter, File, UploadFile, HTTPException
from typing import List
import os

router = APIRouter()


# Extension aceppted 
file_supported_geo = {'.geojson', '.shp'}
file_supported_demand = {'.geojson', '.shp', '.csv'}


def validate_file_extension(filename: str, supported_extensions: set) -> bool:
    """Validates that the file has a supported extension."""
    _, ext = os.path.splitext(filename)  
    return ext.lower() in supported_extensions


@router.post("/exploratory_analysis")
def analyste_socio_spatial(
    geo_mesh: UploadFile = File(...),
    demands: UploadFile = File(...)
):
    
# Validation of the geographic mesh
    if not validate_file_extension(geo_mesh.filename,file_supported_geo):
        raise HTTPException(
            status_code=400,
            detail=f"Arquivo de malha geográfica '{geo_mesh.filename}' não suportado. "
                   f"Extensões válidas: {', '.join(file_supported_geo)}."
        )
    
# Validation of socioeconomic data
    if not validate_file_extension(demands.filename,file_supported_demand):
        raise HTTPException(
            status_code=400,
            detail=f"Arquivo de dados socieconomicos '{demands.filename}' não suportado. "
                   f"Extensões válidas: {', '.join(file_supported_demand)}."
        )
    
    return{
        "message": "Arquivos válidos.",
        "geographic_mesh": geo_mesh.filename,
        "socioeconomic_data": demands.filename
    }

