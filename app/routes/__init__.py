from fastapi import APIRouter
from .allocation_route import router as allocation_router
from .knn_route import router as knn_router
from .distance_matrix_route import router as distance_matrix_router
from .exploratory_analysis import router as filter_by_intersection

router = APIRouter()
router.include_router(allocation_router, prefix="/allocation")
router.include_router(knn_router, prefix="/knn")
router.include_router(distance_matrix_router, prefix="/network_analysis")
router.include_router(filter_by_intersection, prefix= "/api/eda/filter")
