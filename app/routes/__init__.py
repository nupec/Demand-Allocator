from fastapi import APIRouter
from .knn_route import router as knn_router

router = APIRouter()
router.include_router(knn_router, prefix="/knn_model")
