from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
from typing import Optional
import asyncio
import logging

# Importe seus módulos existentes
from files_to_drive import main as files_to_drive_main
from ecarta_processor import main as ecarta_processor_main

app = FastAPI(title="FTP to Drive API", version="1.0.0")

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessRequest(BaseModel):
    process_type: str  # "files_to_drive" ou "ecarta_processor"
    config: Optional[dict] = None

class ProcessResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str] = None

@app.get("/")
async def root():
    return {"message": "FTP to Drive API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/process", response_model=ProcessResponse)
async def process_files(request: ProcessRequest, background_tasks: BackgroundTasks):
    """
    Processa arquivos do FTP para o Google Drive
    """
    try:
        if request.process_type == "files_to_drive":
            background_tasks.add_task(run_files_to_drive)
            return ProcessResponse(
                status="started",
                message="Processamento de arquivos iniciado",
                task_id="files_to_drive_task"
            )
        elif request.process_type == "ecarta_processor":
            background_tasks.add_task(run_ecarta_processor)
            return ProcessResponse(
                status="started",
                message="Processamento de e-carta iniciado",
                task_id="ecarta_processor_task"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="Tipo de processo inválido. Use 'files_to_drive' ou 'ecarta_processor'"
            )
    except Exception as e:
        logger.error(f"Erro ao iniciar processamento: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def run_files_to_drive():
    """Executa o processamento de arquivos em background"""
    try:
        logger.info("Iniciando processamento files_to_drive")
        files_to_drive_main()
        logger.info("Processamento files_to_drive concluído")
    except Exception as e:
        logger.error(f"Erro no processamento files_to_drive: {str(e)}")

async def run_ecarta_processor():
    """Executa o processamento de e-carta em background"""
    try:
        logger.info("Iniciando processamento ecarta_processor")
        ecarta_processor_main()
        logger.info("Processamento ecarta_processor concluído")
    except Exception as e:
        logger.error(f"Erro no processamento ecarta_processor: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)