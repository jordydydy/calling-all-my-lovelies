import asyncio
import httpx
from typing import Dict
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

    async def handle_feedback(self, msg: IncomingMessage):
        """
        Mengirim feedback user (Good/Bad) ke Backend.
        """
        payload_str = msg.metadata.get("payload", "")
        
        # Validasi format payload (contoh: good-123)
        if "-" not in payload_str:
            logger.warning(f"Invalid feedback payload format: {payload_str}")
            return

        try:
            feedback_type_raw, answer_id_raw = payload_str.split("-", 1)
        except ValueError:
            logger.warning(f"Gagal parsing payload feedback: {payload_str}")
            return

        is_good = "good" in feedback_type_raw.lower()
        
        # Cari Session ID (Conversation ID)
        session_id = msg.conversation_id or self.repo_conv.get_latest_id(msg.platform_unique_id, msg.platform)
        
        if not session_id:
            logger.warning(f"Gagal kirim feedback: Tidak ada session ID untuk user {msg.platform_unique_id}")
            return

        # Payload ke Backend Feedback API
        backend_payload = {
            "session_id": session_id,
            "feedback": is_good,
            "answer_id": int(answer_id_raw) if answer_id_raw.isdigit() else 0
        }

        url = settings.FEEDBACK_API_URL
        headers = {"Content-Type": "application/json"}
        if settings.CORE_API_KEY:
            headers["X-API-Key"] = settings.CORE_API_KEY

        logger.info(f"Mengirim Feedback ke {url} | Data: {backend_payload}")

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(url, json=backend_payload, headers=headers)
            
            if resp.status_code == 200:
                logger.info("✅ Feedback berhasil dikirim ke Backend.")
            else:
                logger.error(f"❌ Feedback API Error {resp.status_code}: {resp.text}")
                
        except Exception as e:
            logger.error(f"❌ Gagal mengirim feedback: {e}")

    async def process_message(self, msg: IncomingMessage):
        """Alur utama pemrosesan pesan chat."""
        
        # 1. Pilih Adapter
        adapter = self.adapters.get(msg.platform)
        if not adapter:
            logger.warning(f"No adapter found for platform: {msg.platform}")
            return

        # 2. Typing On
        try:
            adapter.send_typing_on(msg.platform_unique_id)
        except Exception:
            pass

        # 3. Resolve ID
        if not msg.conversation_id:
            msg.conversation_id = self.repo_conv.get_active_id(msg.platform_unique_id, msg.platform)

        # 4. Kirim ke Chatbot
        try:
            # Panggil Chatbot (Direct Response)
            response = await self.chatbot.ask(msg.query, msg.conversation_id, msg.platform, msg.platform_unique_id)
        except Exception as e:
            logger.error(f"Critical error during chatbot processing: {e}")
            response = None

        # 5. Typing Off
        try:
            adapter.send_typing_off(msg.platform_unique_id)
        except Exception:
            pass

        if not response or not response.answer:
            return 

        # 6. Kirim Balasan ke User
        send_kwargs = {}
        if msg.platform == "email":
            meta = self.repo_msg.get_email_metadata(response.conversation_id or msg.conversation_id)
            if meta:
                send_kwargs = meta
            elif msg.metadata:
                send_kwargs = {
                    "subject": msg.metadata.get("subject"),
                    "in_reply_to": msg.metadata.get("in_reply_to"),
                    "references": msg.metadata.get("references")
                }

        adapter.send_message(msg.platform_unique_id, response.answer, **send_kwargs)

        # 7. Kirim Tombol Feedback (Jika ada answer_id)
        raw_data = response.raw.get("data", {}) if response.raw else {}
        answer_id = raw_data.get("answer_id")
        
        if answer_id:
            adapter.send_feedback_request(msg.platform_unique_id, answer_id)
            
        # 8. Simpan Metadata Email (Jika perlu)
        if msg.platform == "email" and response.conversation_id and msg.metadata:
            self.repo_msg.save_email_metadata(
                response.conversation_id,
                msg.metadata.get("subject", ""),
                msg.metadata.get("in_reply_to", ""),
                msg.metadata.get("references", ""),
                msg.metadata.get("thread_key", "")
            )