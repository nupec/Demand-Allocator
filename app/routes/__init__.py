from fastapi import APIRouter
from .knn_route import router as knn_router
from .methods_route import router as methods_router

from .localities_route import router as localities_router


router = APIRouter()
router.include_router(knn_router, prefix="/knn_model")
router.include_router(methods_router, prefix="/methods")

router.include_router(localities_router, prefix="/localities")
