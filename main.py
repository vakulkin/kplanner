import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.ad_groups import router as ad_groups_router
from src.api.campaigns import router as campaigns_router
from src.api.companies import router as companies_router
from src.api.keywords import router as keywords_router
from src.core.database import Base, engine
from src.core.settings import DEMO_USER_ID, DEV_MODE, TITLE, VERSION

# Create tables (skip if in testing mode)
if not os.getenv("TESTING"):
    Base.metadata.create_all(bind=engine)

# Initialize FastAPI app
app = FastAPI(
    title=TITLE,
    version=VERSION,
    response_model_exclude_none=True
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(companies_router)
app.include_router(campaigns_router)
app.include_router(ad_groups_router)
app.include_router(keywords_router)

@app.get("/")
async def root():
    return {
        "message": "Welcome to KPlanner API",
        "mode": "development" if DEV_MODE else "production",
        "demo_user": DEMO_USER_ID if DEV_MODE else None
    }
