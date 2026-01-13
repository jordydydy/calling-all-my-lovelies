import threading
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.core.logging import setup_logging
from app.repositories.base import Database
from app.api.routes import router as api_router
from app.adapters.email.listener import start_email_listener
from app.services.scheduler import run_scheduler
from app.api.dependencies import _chatbot_client 
import logging

setup_logging()
logger = logging.getLogger("main")

def _setup_email_listener():
    is_listener_running = False
    for t in threading.enumerate():
        if t.name == "EmailListenerThread":
            is_listener_running = True
            break
    
    if not is_listener_running and settings.EMAIL_PROVIDER != "unknown":
        email_thread = threading.Thread(
            target=start_email_listener, 
            name="EmailListenerThread", 
            daemon=True
        )
        email_thread.start()
        logger.info("Email Listener Thread Started")
    elif is_listener_running:
        logger.warning("Email Listener already running, skipping start.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    Database.initialize()
    
    scheduler_task = None
    worker_tasks = []
    
    if settings.ENABLE_BACKGROUND_WORKER:
        _setup_email_listener()
        
        scheduler_task = asyncio.create_task(run_scheduler())
        
        logger.info(f"Starting {settings.BACKEND_CONCURRENCY_LIMIT} Redis Workers...")
        for i in range(settings.BACKEND_CONCURRENCY_LIMIT):
            task = asyncio.create_task(_chatbot_client.worker(worker_id=i+1))
            worker_tasks.append(task)
    
    yield
    
    try:
        if scheduler_task:
            scheduler_task.cancel()
            await scheduler_task
            
        for task in worker_tasks:
            task.cancel()
            
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            
    finally:
        Database.close()
        if _chatbot_client.redis:
            await _chatbot_client.redis.close()

app = FastAPI(
    title=settings.APP_NAME,
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/health")
def health():
    return {"status": "ok"}