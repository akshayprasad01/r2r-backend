from fastapi import APIRouter
from app.api.api_v1.transformation import transformation
from app.api.api_v1.recon import recon
from app.api.api_v1.preview import preview

api_router = APIRouter(prefix="/aira-transformation")
api_router.include_router(transformation.router, prefix="/api/v1/transformation", tags=["transformation"])
api_router.include_router(preview.router, prefix="/api/v1/preview", tags=["preview"])
api_router.include_router(recon.router, prefix="/api/v1/recon", tags=["recon"])
