from fastapi import FastAPI
from app.config import settings
from app.routes import router as api_router

app = FastAPI(
  title=settings.APP_TITLE,
  description=settings.APP_DESCRIPTION,
  version=settings.APP_VERSION,
)

app.include_router(api_router)
