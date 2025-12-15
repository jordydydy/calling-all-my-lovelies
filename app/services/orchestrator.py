import asyncio
import httpx
import uuid
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

    async def timeout_session(self, conversation_id: str, platform: str, user_id: str):
        adapter = self.adapters.get(platform)
        if not adapter: return
        logger.info(f"TIMEOUT: Auto-closing session {conversation_id} for {platform} user {user_id}")
        
        try:
            await self.chatbot.ask(query="Terima Kasih", conversation_id=conversation_id, platform=platform, user_id=user_id)
        except Exception as e:
            logger.error(f"Failed to send close signal to AI: {e}")

        closing_text = (
            "Untuk keamanan dan kenyamanan Anda, sesi ini telah diakhiri. "
            "Silakan mulai percakapan kembali dari awal jika membutuhkan bantuan."
        )
        
        send_kwargs = {}
        if platform == "email":
            send_kwargs = {"subject": "Session Ended"}
            meta = self.repo_msg.get_email_metadata(conversation_id)
            if meta:
                if settings.EMAIL_PROVIDER == "azure_oauth2":
                    send_kwargs["graph_message_id"] = meta.get("graph_message_id")
                else:
                    send_kwargs.update(meta)

        await adapter.send_message(user_id, closing_text, **send_kwargs)
        self.repo_conv.close_session(conversation_id)

    async def handle_feedback(self, msg: IncomingMessage):
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
        url = settings.FEEDBACK_API_URL
        headers = {"Content-Type": "application/json"}
        if settings.CORE_API_KEY: headers["X-API-Key"] = settings.CORE_API_KEY
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(url, json=backend_payload, headers=headers)
        except Exception as e:
            logger.error(f"Gagal kirim feedback: {e}")

    def _get_email_send_kwargs(self, conversation_id: str) -> Dict:
        if not conversation_id:
            return {"subject": "Re: Your Inquiry"}
            
        meta = self.repo_msg.get_email_metadata(conversation_id)
        if meta: 
            if settings.EMAIL_PROVIDER == "azure_oauth2":
                return {
                    "subject": meta.get("subject"),
                    "graph_message_id": meta.get("graph_message_id")  # Reply to this message
                }
            return {
                "subject": meta.get("subject"),
                "in_reply_to": meta.get("in_reply_to"),
                "references": meta.get("references")
            }
            
        return {"subject": "Re: Your Inquiry"}

    async def send_manual_message(self, data: dict):
        payload = data.get("data") if "data" in data else data
        user_id = payload.get("user") or payload.get("platform_unique_id") or payload.get("recipient_id") or payload.get("user_id")
        platform = payload.get("platform")
        answer = payload.get("answer") or payload.get("message")
        conversation_id = payload.get("conversation_id")
        answer_id = payload.get("answer_id")
        is_helpdesk = payload.get("is_helpdesk", False)
        
        if not user_id or not answer or not platform: 
            logger.warning(f"Invalid callback payload: {payload}")
            return
            
        adapter = self.adapters.get(platform)
        if not adapter: return
        
        send_kwargs = {}
        if platform == "email":
            send_kwargs = self._get_email_send_kwargs(conversation_id)
        
        await adapter.send_message(user_id, answer, **send_kwargs)
        
        try: 
            await adapter.send_typing_off(user_id)
        except Exception: 
            pass
        
        if answer_id and not is_helpdesk: 
            await adapter.send_feedback_request(user_id, answer_id)

    def _ensure_conversation_id(self, msg: IncomingMessage):
        """
        Determine conversation_id for the message
        For Azure email: Use conversationId from metadata to link replies
        For IMAP email: Use thread_key
        For other platforms: Use active session or generate new
        """
        if msg.platform == "email" and msg.metadata:
            # Azure Office365 - use conversationId for threading
            if settings.EMAIL_PROVIDER == "azure_oauth2" and msg.metadata.get("conversation_id"):
                azure_conv_id = msg.metadata.get("conversation_id")
                
                # Check if we already have this conversation in DB
                existing_id = self.repo_msg.get_conversation_by_azure_thread(azure_conv_id)
                if existing_id:
                    msg.conversation_id = existing_id
                    logger.info(f"AZURE THREAD MATCH: Found existing session {existing_id}")
                    return
                
                # Generate deterministic UUID from Azure conversationId
                msg.conversation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, azure_conv_id))
                logger.info(f"NEW AZURE THREAD: Generated ID {msg.conversation_id} from {azure_conv_id}")
                return
            
            # IMAP/Gmail - use thread_key (References/In-Reply-To)
            elif msg.metadata.get("thread_key"):
                thread_key = msg.metadata.get("thread_key")
                existing_id = self.repo_msg.get_conversation_by_thread(thread_key)
                if existing_id:
                    msg.conversation_id = existing_id
                    logger.info(f"IMAP THREAD MATCH: Found existing session {existing_id}")
                    return
                
                msg.conversation_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, thread_key))
                logger.info(f"NEW IMAP THREAD: Generated ID {msg.conversation_id}")
                return
        
        # WhatsApp/Instagram - check active session
        if not msg.conversation_id and msg.platform != "email":
            msg.conversation_id = self.repo_conv.get_active_id(msg.platform_unique_id, msg.platform)

        # Generate new ID if nothing found
        if not msg.conversation_id:
            msg.conversation_id = str(uuid.uuid4())
            logger.info(f"GENERATED RANDOM ID: {msg.conversation_id}")

    def _save_email_metadata(self, msg: IncomingMessage):
        """Save email metadata for threading replies"""
        if msg.platform != "email" or not msg.conversation_id or not msg.metadata:
            return
            
        if settings.EMAIL_PROVIDER == "azure_oauth2":
            # For Azure, save graph_message_id and azure conversation_id
            self.repo_msg.save_email_metadata(
                msg.conversation_id,
                msg.metadata.get("subject", ""),
                msg.metadata.get("graph_message_id", ""),  # This is what we reply to
                "",  # No references in Azure
                msg.metadata.get("conversation_id", "")  # Azure's conversationId for threading
            )
        else:
            # For IMAP, save traditional email headers
            self.repo_msg.save_email_metadata(
                msg.conversation_id,
                msg.metadata.get("subject", ""),
                msg.metadata.get("in_reply_to", ""),
                msg.metadata.get("references", ""),
                msg.metadata.get("thread_key", "")
            )

    async def process_message(self, msg: IncomingMessage):
        adapter = self.adapters.get(msg.platform)
        if not adapter: return

        try:
            msg_id = msg.metadata.get("message_id") if msg.metadata else None
            await adapter.send_typing_on(msg.platform_unique_id, message_id=msg_id)
        except Exception:
            pass

        if not msg.conversation_id:
            self._ensure_conversation_id(msg)

        self._save_email_metadata(msg)

        success = await self.chatbot.ask(msg.query, msg.conversation_id, msg.platform, msg.platform_unique_id)
        
        if not success:
            logger.error("Gagal push ke backend AI.")
            try: 
                await adapter.send_typing_off(msg.platform_unique_id)
            except Exception: 
                pass