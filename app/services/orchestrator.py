import httpx
import uuid
import re
from typing import Dict, Optional
from app.schemas.models import IncomingMessage
from app.repositories.conversation import ConversationRepository
from app.repositories.message import MessageRepository
from app.services.chatbot import ChatbotClient
from app.adapters.base import BaseAdapter
from app.core.config import settings
import logging

logger = logging.getLogger("service.orchestrator")

class MessageOrchestrator:
    def __init__(
        self, 
        repo_conv: ConversationRepository,
        repo_msg: MessageRepository,
        chatbot: ChatbotClient,
        adapters: Dict[str, BaseAdapter]
    ):
        self.repo_conv = repo_conv
        self.repo_msg = repo_msg
        self.chatbot = chatbot
        self.adapters = adapters

    async def timeout_session(self, conversation_id: str, platform: str, user_id: str):
        adapter = self.adapters.get(platform)
        if not adapter: return
        if self.repo_conv.is_helpdesk_session(conversation_id): return
        logger.info(f"TIMEOUT: Auto-closing session {conversation_id}")
        try:
            await self.chatbot.ask("Terima Kasih", conversation_id, platform, user_id)
        except Exception: pass
        
        closing_text = "Untuk keamanan dan kenyamanan Anda, sesi ini telah diakhiri. Silakan mulai percakapan kembali dari awal."
        send_kwargs = {}
        if platform == "email":
            meta = self.repo_msg.get_email_metadata(conversation_id)
            if meta: send_kwargs = meta
            send_kwargs["subject"] = "Session Ended"
            
        await adapter.send_message(user_id, closing_text, **send_kwargs)
        self.repo_conv.close_session(conversation_id)

    async def handle_feedback(self, msg: IncomingMessage):
        # (Keep existing code)
        payload_str = msg.metadata.get("payload", "")
        if "-" not in payload_str: return
        try:
            feedback_type_raw, answer_id_raw = payload_str.split("-", 1)
        except ValueError: return
        
        is_good = "good" in feedback_type_raw.lower()
        session_id = msg.conversation_id or self.repo_conv.get_latest_id(msg.platform_unique_id, msg.platform)
        if not session_id: return
        
        backend_payload = {
            "session_id": session_id,
            "feedback": is_good,
            "answer_id": int(answer_id_raw) if answer_id_raw.isdigit() else 0
        }
        url = settings.BACKEND_FEEDBACK_URL
        headers = {"Content-Type": "application/json", "X-API-Key": settings.BACKEND_API_KEY}
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json=backend_payload, headers=headers)
        except Exception as e:
            logger.error(f"Gagal kirim feedback: {e}")

    def _get_email_send_kwargs(self, conversation_id: str) -> Dict:
        if not conversation_id: return {"subject": "Re: Your Inquiry"}
        meta = self.repo_msg.get_email_metadata(conversation_id)
        if meta: 
            if settings.EMAIL_PROVIDER == "azure_oauth2":
                return {"subject": meta.get("subject"), "graph_message_id": meta.get("graph_message_id")}
            return {"subject": meta.get("subject"), "in_reply_to": meta.get("in_reply_to"), "references": meta.get("references")}
        return {"subject": "Re: Your Inquiry"}

    async def send_manual_message(self, data: dict):
        payload = data.get("data") if "data" in data else data
        user_id = payload.get("user") or payload.get("platform_unique_id") or payload.get("recipient_id") or payload.get("user_id")
        platform = payload.get("platform")
        answer = payload.get("answer") or payload.get("message")
        conversation_id = payload.get("conversation_id")
        answer_id = payload.get("answer_id")
        is_helpdesk = payload.get("is_helpdesk", False)
        
        if user_id:
            await self.chatbot.decrement_user_counter(user_id)

        if not user_id or not answer or not platform: 
            logger.warning(f"Invalid callback payload: {payload}")
            return
            
        adapter = self.adapters.get(platform)
        if not adapter: return
        
        send_kwargs = {}
        if platform == "email":
            send_kwargs = self._get_email_send_kwargs(conversation_id)
        
        await adapter.send_message(user_id, answer, **send_kwargs)
        
        try: await adapter.send_typing_off(user_id)
        except Exception: pass
        
        is_busy_message = "peningkatan jumlah pesan" in answer
        if answer_id and not is_helpdesk and not is_busy_message: 
            await adapter.send_feedback_request(user_id, answer_id)

    def _check_helpdesk_session(self, msg: IncomingMessage) -> Optional[str]:
        if msg.platform == "email": return None
        active_id = self.repo_conv.get_active_id(msg.platform_unique_id, msg.platform)
        if active_id and self.repo_conv.is_helpdesk_session(active_id): return active_id
        return None

    def _ensure_conversation_id(self, msg: IncomingMessage):
        if msg.platform == "email":
            self._handle_email_conversation_id(msg)
            return
        helpdesk_id = self._check_helpdesk_session(msg)
        if helpdesk_id:
            msg.conversation_id = helpdesk_id
            return
        if not msg.conversation_id:
            msg.conversation_id = self.repo_conv.get_active_id(msg.platform_unique_id, msg.platform)
        if not msg.conversation_id:
            msg.conversation_id = str(uuid.uuid4())

    def _handle_email_conversation_id(self, msg: IncomingMessage):
        if not msg.metadata:
            msg.conversation_id = str(uuid.uuid4())
            return
        if settings.EMAIL_PROVIDER == "azure_oauth2":
            self._handle_azure_email_thread(msg)
        else:
            self._handle_standard_email_thread(msg)

    def _handle_azure_email_thread(self, msg: IncomingMessage):
        azure_conv_id = msg.metadata.get("conversation_id")
        if azure_conv_id:
            existing_id = self.repo_msg.get_conversation_by_azure_thread(azure_conv_id)
            if existing_id: msg.conversation_id = existing_id
            else: msg.conversation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, azure_conv_id))
        else: msg.conversation_id = str(uuid.uuid4())

    def _handle_standard_email_thread(self, msg: IncomingMessage):
        thread_key = msg.metadata.get("thread_key")
        if thread_key:
            existing_id = self.repo_msg.get_conversation_by_thread(thread_key)
            if existing_id: 
                msg.conversation_id = existing_id
                return
        sender = msg.platform_unique_id.lower()
        subject = msg.metadata.get("subject", "").lower().strip()
        clean_subject = re.sub(r'^(re:|fwd:|balas:|tr:|aw:)\s*', '', subject, flags=re.IGNORECASE).strip()
        seed = f"{sender}|{clean_subject}"
        msg.conversation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, seed))

    def _save_email_metadata(self, msg: IncomingMessage):
        if msg.platform != "email" or not msg.conversation_id or not msg.metadata: return
        if settings.EMAIL_PROVIDER == "azure_oauth2":
            self.repo_msg.save_email_metadata(msg.conversation_id, msg.metadata.get("subject", ""), msg.metadata.get("graph_message_id", ""), "", msg.metadata.get("conversation_id", ""))
        else:
            self.repo_msg.save_email_metadata(msg.conversation_id, msg.metadata.get("subject", ""), msg.metadata.get("message_id", ""), msg.metadata.get("references", ""), msg.metadata.get("thread_key", ""))

    async def process_message(self, msg: IncomingMessage):
        adapter = self.adapters.get(msg.platform)
        if not adapter: return

        if not msg.conversation_id:
            self._ensure_conversation_id(msg)

        self._save_email_metadata(msg)

        try:
            msg_id = msg.metadata.get("message_id") if msg.metadata else None
            await adapter.send_typing_on(msg.platform_unique_id, message_id=msg_id)
            if msg.platform == "whatsapp" and msg_id and hasattr(adapter, 'mark_as_read'):
                await adapter.mark_as_read(msg_id)
        except Exception: pass

        status = await self.chatbot.ask(
            msg.query, 
            msg.conversation_id, 
            msg.platform, 
            msg.platform_unique_id
        )
        
        if status == "blocked":
            logger.info(f"Refusing message from {msg.platform_unique_id} (Rate Limit Exceeded)")
            
            spam_warning = "Mohon maaf, Anda mengirim terlalu banyak pesan sekaligus. Mohon tunggu balasan kami sebelum mengirim pesan berikutnya."
            
            try:
                send_kwargs = {}
                if msg.platform == "email":
                    send_kwargs = self._get_email_send_kwargs(msg.conversation_id)
                    
                await adapter.send_message(msg.platform_unique_id, spam_warning, **send_kwargs)
                await adapter.send_typing_off(msg.platform_unique_id)
            except Exception as e:
                logger.error(f"Failed to send spam warning: {e}")
                
        elif status == "error":
            logger.error(f"Failed to enqueue message for {msg.conversation_id}")
            try: await adapter.send_typing_off(msg.platform_unique_id)
            except Exception: pass