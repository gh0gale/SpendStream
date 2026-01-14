from fastapi import FastAPI


from .database.init import Base, engine
from .routes.init import ingestion_router, upload_router
from .routes.init import internal_files_router

Base.metadata.create_all(bind=engine)

app = FastAPI(title="SpendStream Backend v0")

app.include_router(upload_router, prefix="/upload", tags=["Upload"])
app.include_router(ingestion_router, prefix="/ingestion", tags=["Ingestion"])
app.include_router(internal_files_router, prefix="/internal/file", tags=["Internal"])



@app.get("/")
def health_check():
    return {"status": "Backend v0 running"}
