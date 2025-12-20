from fastapi import APIRouter, Depends, BackgroundTasks, Request, Query, Response, HTTPException
from app.core.config import settings
from app.schemas.models import IncomingMessage
from app.api.dependencies import get_orchestrator
from app.services.orchestrator import MessageOrchestrator
from app.services.parsers import parse_whatsapp_payload, parse_instagram_payload
from app.repositories.message import MessageRepository
import logging

logger = logging.getLogger("api.routes")
router = APIRouter()

_msg_repo = MessageRepository()

@router.get("/whatsapp/webhook")
def verify_whatsapp(
    mode: str = Query(..., alias="hub.mode"),
    token: str = Query(..., alias="hub.verify_token"),
    challenge: str = Query(..., alias="hub.challenge"),
):
    if mode == "subscribe" and token == settings.WHATSAPP_VERIFY_TOKEN:
        return Response(content=challenge, media_type="text/plain")
    raise HTTPException(status_code=403, detail="Verification failed")

@router.get("/instagram/webhook")
def verify_instagram(
    mode: str = Query(..., alias="hub.mode"),
    token: str = Query(..., alias="hub.verify_token"),
    challenge: str = Query(..., alias="hub.challenge"),
):
    if mode == "subscribe" and token == settings.INSTAGRAM_VERIFY_TOKEN:
        return Response(content=challenge, media_type="text/plain")
    raise HTTPException(status_code=403, detail="Verification failed")

@router.post("/whatsapp/webhook")
async def whatsapp_webhook(
    request: Request,
    bg_tasks: BackgroundTasks,
    orchestrator: MessageOrchestrator = Depends(get_orchestrator)
):
    data = await request.json()
    msg = parse_whatsapp_payload(data)
    
    if msg:
        if msg.metadata and msg.metadata.get("is_feedback"):
            logger.info(f"Feedback Event Received (WA): {msg.metadata['payload']}")
            bg_tasks.add_task(orchestrator.handle_feedback, msg)
        else:
            bg_tasks.add_task(orchestrator.process_message, msg)
            
    return {"status": "ok"}

@router.post("/instagram/webhook")
async def instagram_webhook(
    request: Request,
    bg_tasks: BackgroundTasks,
    orchestrator: MessageOrchestrator = Depends(get_orchestrator)
):
    data = await request.json()
    msg = parse_instagram_payload(data)
    
    if msg:
        if msg.metadata and msg.metadata.get("is_feedback"):
            logger.info(f"Feedback Event Received (IG): {msg.metadata['payload']}")
            bg_tasks.add_task(orchestrator.handle_feedback, msg)
        else:
            bg_tasks.add_task(orchestrator.process_message, msg)
            
    return {"status": "ok"}

@router.post("/api/send/reply")
async def receive_backend_reply(
    request: Request,
    bg_tasks: BackgroundTasks,
    orchestrator: MessageOrchestrator = Depends(get_orchestrator)
):
    data = await request.json()
    logger.info(f"Received reply callback from Backend: {data}")
    
    bg_tasks.add_task(orchestrator.send_manual_message, data)
    
    return {"status": "processed"}

@router.post("/api/messages/process")
async def process_message_internal(
    msg: IncomingMessage,
    bg_tasks: BackgroundTasks,
    orchestrator: MessageOrchestrator = Depends(get_orchestrator)
):
    if msg.platform == "email" and msg.metadata:
        unique_id = msg.metadata.get("graph_message_id") or msg.metadata.get("message_id")
        
        if unique_id and _msg_repo.is_processed(unique_id, "email"):
            logger.info(f"Duplicate email blocked: {unique_id}")
            return {"status": "duplicate", "message": "Already processed"}
    
    bg_tasks.add_task(orchestrator.process_message, msg)
    return {"status": "queued"}