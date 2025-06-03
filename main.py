from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
import time
import asyncio
import logging
import traceback
from typing import Optional
from concurrent.futures import ThreadPoolExecutor
import tempfile
import shutil

# Importe seus módulos existentes
from files_to_drive import main as files_to_drive_main
from ecarta_processor import main as ecarta_processor_main

app = FastAPI(title="FTP to Drive API", version="1.0.0")

# ✅ Configurar logging mais detalhado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ✅ Executor para tarefas síncronas
executor = ThreadPoolExecutor(max_workers=2)

class ProcessRequest(BaseModel):
    process_type: str  # "files_to_drive" ou "ecarta_processor"
    config: Optional[dict] = None

class ProcessResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str] = None
    details: Optional[dict] = None

# ✅ Variável global para armazenar status das tarefas
task_status = {}

@app.get("/")
async def root():
    return {
        "message": "FTP to Drive API is running",
        "version": "1.0.0",
        "environment": "Vercel" if os.getenv("VERCEL") else "Local"
    }

@app.get("/health")
async def health_check():
    """Health check com informações do sistema"""
    try:
        # ✅ Verificar diretório temporário
        temp_dir = tempfile.gettempdir()
        temp_writable = os.access(temp_dir, os.W_OK)
        
        # ✅ Verificar variáveis de ambiente críticas
        env_vars = {
            "HOST": bool(os.getenv('HOST')),
            "USER_ECARTA": bool(os.getenv('USER_ECARTA')),
            "PASSWORD": bool(os.getenv('PASSWORD')),
            "TARGET_FOLDER_ID": bool(os.getenv('TARGET_FOLDER_ID')),
            "GOOGLE_CREDENTIALS": bool(os.getenv('GOOGLE_CREDENTIALS'))
        }
        
        return {
            "status": "healthy",
            "temp_dir": temp_dir,
            "temp_writable": temp_writable,
            "environment_vars": env_vars,
            "active_tasks": len(task_status)
        }
    except Exception as e:
        logger.error(f"Erro no health check: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.get("/tasks")
async def get_tasks():
    """Retorna status de todas as tarefas"""
    return {"tasks": task_status}

@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Retorna status de uma tarefa específica"""
    if task_id not in task_status:
        raise HTTPException(status_code=404, detail="Tarefa não encontrada")
    return {"task_id": task_id, "status": task_status[task_id]}

@app.post("/process", response_model=ProcessResponse)
async def process_files(request: ProcessRequest, background_tasks: BackgroundTasks):
    """
    Processa arquivos do FTP para o Google Drive
    """
    try:
        # ✅ Gerar ID único para a tarefa
        import time
        task_id = f"{request.process_type}_{int(time.time())}"
        
        # ✅ Inicializar status da tarefa
        task_status[task_id] = {
            "status": "started",
            "message": "Tarefa iniciada",
            "start_time": time.time(),
            "process_type": request.process_type
        }
        
        if request.process_type == "files_to_drive":
            background_tasks.add_task(run_files_to_drive_safe, task_id)
            return ProcessResponse(
                status="started",
                message="Processamento de arquivos iniciado",
                task_id=task_id,
                details={"process_type": "files_to_drive"}
            )
        elif request.process_type == "ecarta_processor":
            background_tasks.add_task(run_ecarta_processor_safe, task_id)
            return ProcessResponse(
                status="started",
                message="Processamento de e-carta iniciado",
                task_id=task_id,
                details={"process_type": "ecarta_processor"}
            )
        else:
            # ✅ Remover tarefa inválida do status
            del task_status[task_id]
            raise HTTPException(
                status_code=400,
                detail="Tipo de processo inválido. Use 'files_to_drive' ou 'ecarta_processor'"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao iniciar processamento: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def run_files_to_drive_safe(task_id: str):
    """Executa o processamento de arquivos em background com tratamento de erros"""
    try:
        logger.info(f"[{task_id}] Iniciando processamento files_to_drive")
        
        # ✅ Atualizar status
        task_status[task_id].update({
            "status": "running",
            "message": "Processamento em andamento"
        })
        
        # ✅ Executar função síncrona em thread separada
        loop = asyncio.get_event_loop()
        resultado = await loop.run_in_executor(executor, files_to_drive_main)
        
        # ✅ Atualizar status com resultado
        task_status[task_id].update({
            "status": "completed",
            "message": "Processamento concluído com sucesso",
            "end_time": time.time(),
            "result": resultado
        })
        
        logger.info(f"[{task_id}] Processamento files_to_drive concluído")
        
    except Exception as e:
        error_msg = str(e)
        error_traceback = traceback.format_exc()
        
        logger.error(f"[{task_id}] Erro no processamento files_to_drive: {error_msg}")
        logger.error(f"[{task_id}] Traceback: {error_traceback}")
        
        # ✅ Atualizar status com erro
        task_status[task_id].update({
            "status": "error",
            "message": f"Erro no processamento: {error_msg}",
            "end_time": time.time(),
            "error": error_msg,
            "traceback": error_traceback
        })

async def run_ecarta_processor_safe(task_id: str):
    """Executa o processamento de e-carta em background com tratamento de erros"""
    try:
        logger.info(f"[{task_id}] Iniciando processamento ecarta_processor")
        
        # ✅ Atualizar status
        task_status[task_id].update({
            "status": "running",
            "message": "Processamento em andamento"
        })
        
        # ✅ Executar função síncrona em thread separada
        loop = asyncio.get_event_loop()
        resultado = await loop.run_in_executor(executor, ecarta_processor_main)
        
        # ✅ Atualizar status com resultado
        task_status[task_id].update({
            "status": "completed",
            "message": "Processamento concluído com sucesso",
            "end_time": time.time(),
            "result": resultado
        })
        
        logger.info(f"[{task_id}] Processamento ecarta_processor concluído")
        
    except Exception as e:
        error_msg = str(e)
        error_traceback = traceback.format_exc()
        
        logger.error(f"[{task_id}] Erro no processamento ecarta_processor: {error_msg}")
        logger.error(f"[{task_id}] Traceback: {error_traceback}")
        
        # ✅ Atualizar status com erro
        task_status[task_id].update({
            "status": "error",
            "message": f"Erro no processamento: {error_msg}",
            "end_time": time.time(),
            "error": error_msg,
            "traceback": error_traceback
        })

# ✅ Endpoint para limpeza manual de tarefas antigas
@app.delete("/tasks/cleanup")
async def cleanup_old_tasks():
    """Remove tarefas antigas do status"""
    import time
    current_time = time.time()
    old_tasks = []
    
    for task_id, task_info in list(task_status.items()):
        # Remover tarefas com mais de 1 hora
        if current_time - task_info.get("start_time", 0) > 3600:
            old_tasks.append(task_id)
            del task_status[task_id]
    
    return {
        "message": f"Limpeza concluída: {len(old_tasks)} tarefa(s) removida(s)",
        "removed_tasks": old_tasks
    }

# ✅ Endpoint para teste rápido
@app.post("/test")
async def test_environment():
    """Testa o ambiente sem executar processamento completo"""
    try:
        # Testar criação de diretório temporário
        temp_dir = tempfile.mkdtemp()
        test_file = os.path.join(temp_dir, "test.txt")
        
        with open(test_file, "w") as f:
            f.write("teste")
        
        file_exists = os.path.exists(test_file)
        
        # Limpar
        shutil.rmtree(temp_dir)
        
        return {
            "status": "success",
            "message": "Ambiente testado com sucesso",
            "temp_dir_writable": True,
            "file_creation": file_exists
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Erro no teste: {str(e)}",
            "temp_dir_writable": False
        }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)