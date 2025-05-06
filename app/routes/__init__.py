from fastapi import APIRouter

from .eda_allocation_route import router as eda_allocation_router   # 1º – define tudo que knn vai usar
from .knn_route           import router as knn_router              # 2º – importa eda_allocation internamente
from .consulta_base       import router as consulta_router         # 3º

router = APIRouter()
router.include_router(knn_router,           prefix="/knn_model")
router.include_router(eda_allocation_router, prefix="/api/eda")
router.include_router(consulta_router)
