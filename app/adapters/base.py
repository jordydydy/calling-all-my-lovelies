from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import asyncio

class BaseAdapter(ABC):
    @abstractmethod
    async def send_message(self, recipient_id: str, text: str, **kwargs) -> Dict[str, Any]:
        pass

    async def send_typing_on(self, recipient_id: str, message_id: Optional[str] = None):
        # Base implementation for turning off typing indicators.
        # Left empty as not all platforms support or require an explicit 'off' signal.
        pass

    async def send_typing_off(self, recipient_id: str):
        # Base implementation for turning off typing indicators.
        # Left empty as not all platforms support or require an explicit 'off' signal.
        pass
    
    async def send_feedback_request(self, recipient_id: str, answer_id: int) -> Dict[str, Any]:
        await asyncio.sleep(0)
        return {"sent": False, "reason": "Not implemented"}