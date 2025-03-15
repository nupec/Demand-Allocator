from fastapi import APIRouter
from .knn_route import router as knn_router
from .allocate_by_city_list import router as city_router

router = APIRouter()
router.include_router(knn_router, prefix="/knn_model")
router.include_router(city_router, prefix="/knn_model")