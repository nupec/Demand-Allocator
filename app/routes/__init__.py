from fastapi import APIRouter
from .allocation_route import router as allocation_router
from .knn_route import router as knn_router

router = APIRouter()
router.include_router(allocation_router, prefix="/allocation")
router.include_router(knn_router, prefix="/knn")
