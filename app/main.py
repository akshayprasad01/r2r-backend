from fastapi import FastAPI, Request, Response
import json
from dotenv import load_dotenv
load_dotenv()
from app.api.api_v1.router import api_router
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response
from app.helpers.iw_cache import initialize_cache
from app.services.app import AppService
from app.logger import logger
import os
import uvicorn
# Load environment variables from .env

app = FastAPI(
    title = "R2R Transformation App",
    description = "This is the R2R Transformation App API description",
    version = "1.0",
    docs_url = "/aira-transformation",
    openapi_url = "/api/v1/openapi.json"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security_headers = os.getenv("SECURITY_RESPONSE_HEADERS", "{\"X-Content-Type-Options\":\"nosniff\",\"X-Frame-Options\":\"DENY\",\"Content-Security-Policy\":\"default-src 'none';\"}")
security_headers = json.loads(security_headers)
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response: Response = await call_next(request)
    # Add security headers
    for header, value in security_headers.items():
        response.headers[header] = value
    return response

app_service = AppService()

@app.on_event("startup")
async def startup_event():
    """
    Startup event to fetch data and set cache.
    """
    initialize_cache()
    logger.debug("Fetch tenant information and store it in the cache - Started")
    app_service.fetch_tenants_and_set_cache()
    logger.debug("Fetch tenant information and store it in the cache - Finished")

app.include_router(api_router)