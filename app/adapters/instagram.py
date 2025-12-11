import re
from app.core.config import settings
from app.adapters.base import BaseAdapter
from app.adapters.utils import split_text_smartly, make_meta_request

class InstagramAdapter(BaseAdapter):
    def __init__(self):
        self.version = "v24.0"
        self.base_url = f"https://graph.instagram.com/{self.version}/{settings.INSTAGRAM_CHATBOT_ID}/messages"
        self.token = settings.INSTAGRAM_PAGE_ACCESS_TOKEN

    def _clean_id(self, user_id: str) -> str:
        return user_id.replace('@instagram.com', '').strip()

    async def send_typing_on(self, recipient_id: str, message_id: str = None):
        if not self.token: return
        payload = {"recipient": {"id": self._clean_id(recipient_id)}, "sender_action": "typing_on"}
        await make_meta_request("POST", self.base_url, self.token, payload)

    async def send_typing_off(self, recipient_id: str):
        if not self.token: return
        payload = {"recipient": {"id": self._clean_id(recipient_id)}, "sender_action": "typing_off"}
        await make_meta_request("POST", self.base_url, self.token, payload)

    async def send_message(self, recipient_id: str, text: str, **kwargs):
        if not self.token: return {"success": False}
        
        text = re.sub(r'\*\*(.*?)\*\*', r'*\1*', text)
        chunks = split_text_smartly(text, 1000)
        
        results = []
        for chunk in chunks:
            payload = {
                "recipient": {"id": self._clean_id(recipient_id)},
                "message": {"text": chunk}
            }
            res = await make_meta_request("POST", self.base_url, self.token, payload)
            results.append(res)
            
        return {"sent": True, "results": results}

    async def send_feedback_request(self, recipient_id: str, answer_id: int):
        payload = {
            "recipient": {"id": self._clean_id(recipient_id)},
            "message": {
                "text": "Apakah jawaban ini membantu?",
                "quick_replies": [
                    {"content_type": "text", "title": "Yes", "payload": f"good-{answer_id}"},
                    {"content_type": "text", "title": "No", "payload": f"bad-{answer_id}"}
                ]
            }
        }
        return await make_meta_request("POST", self.base_url, self.token, payload)