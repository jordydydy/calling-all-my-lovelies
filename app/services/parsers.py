from typing import Dict, Any, Optional, Tuple
from app.schemas.models import IncomingMessage
from app.core.config import settings

def parse_whatsapp_payload(data: Dict[str, Any]) -> Optional[IncomingMessage]:
    """Ekstrak pesan dari JSON WhatsApp Cloud API."""
    try:
        entry = data.get("entry", [])[0]
        changes = entry.get("changes", [])[0]
        value = changes.get("value", {})
        
        if "messages" not in value:
            return None
            
        message = value["messages"][0]
        sender_id = message.get("from")

        # Abaikan pesan dari diri sendiri (Loop Prevention)
        if str(sender_id) == str(settings.WHATSAPP_PHONE_NUMBER_ID):
            return None

        msg_type = message.get("type")
        
        # Handle Text Message
        if msg_type == "text":
            return IncomingMessage(
                platform_unique_id=sender_id,
                query=message["text"]["body"],
                platform="whatsapp",
                metadata={"phone": sender_id}
            )
            
        # Handle Button Reply
        elif msg_type == "interactive":
            interactive = message.get("interactive", {})
            if interactive.get("type") == "button_reply":
                btn_id = interactive["button_reply"]["id"]
                return IncomingMessage(
                    platform_unique_id=sender_id,
                    query=f"FEEDBACK_EVENT:{btn_id}",
                    platform="whatsapp",
                    metadata={"is_feedback": True, "payload": btn_id}
                )
                
    except (IndexError, KeyError, AttributeError):
        pass
    return None

def parse_instagram_payload(data: Dict[str, Any]) -> Optional[IncomingMessage]:
    """Ekstrak pesan dari JSON Instagram Webhook."""
    try:
        entry = data.get("entry", [])[0]
        messaging = entry.get("messaging", [])[0]
        
        sender_id = messaging.get("sender", {}).get("id")
        
        # Abaikan pesan dari diri sendiri (Loop Prevention)
        if str(sender_id) == str(settings.INSTAGRAM_CHATBOT_ID):
            return None

        message = messaging.get("message", {})
        
        # [FIX] PRIORITAS 1: Cek Quick Reply (Feedback) DULUAN!
        # Instagram mengirim 'text' DAN 'quick_reply' secara bersamaan saat tombol ditekan.
        # Jika kita cek 'text' duluan, feedback tidak akan pernah terdeteksi.
        if "quick_reply" in message:
            payload = message["quick_reply"].get("payload")
            return IncomingMessage(
                platform_unique_id=sender_id,
                query=f"FEEDBACK_EVENT:{payload}",
                platform="instagram",
                metadata={"is_feedback": True, "payload": payload}
            )

        # [FIX] PRIORITAS 2: Baru cek Text biasa
        if "text" in message:
            # Cek flag echo dari IG
            if message.get("is_echo"):
                return None

            return IncomingMessage(
                platform_unique_id=sender_id,
                query=message["text"],
                platform="instagram"
            )
            
    except (IndexError, KeyError, AttributeError):
        pass
    return None