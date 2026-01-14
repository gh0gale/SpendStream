from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database.init import Base, engine
from .routes.init import ingestion_router, upload_router
from .routes.init import internal_files_router, internal_email_router, email_trigger_router

Base.metadata.create_all(bind=engine)

app = FastAPI(title="SpendStream Backend v0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload_router)
app.include_router(ingestion_router)
app.include_router(internal_files_router)
app.include_router(internal_email_router)
app.include_router(email_trigger_router)



@app.get("/")
def health_check():
    return {"status": "Backend v0 running"}
