import logging
from fastapi import FastAPI
from app.config import settings
from app.routes import router as api_router

logger = logging.getLogger(__name__)

app = FastAPI(
  title=settings.APP_TITLE,
  description=settings.APP_DESCRIPTION,
  version=settings.APP_VERSION,
)

logger.info("Starting FastAPI application with title: %s", settings.APP_TITLE)
app.include_router(api_router)
logger.info("Router has been included. Application setup complete.")

