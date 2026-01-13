import httpx
import asyncio
import json
import redis.asyncio as redis
from datetime import datetime, timezone
from app.core.config import settings
import logging

logger = logging.getLogger("service.chatbot")

class ChatbotClient:
    def __init__(self):
        self.redis_url = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
        self.redis = None

    async def _get_redis(self):
        if not self.redis:
            self.redis = redis.from_url(self.redis_url, decode_responses=True)
        return self.redis

    async def _fire_request(self, payload: dict):
        headers = {"Content-Type": "application/json"}
        if settings.BACKEND_API_KEY:
            headers["X-API-Key"] = settings.BACKEND_API_KEY

        url = settings.BACKEND_ASK_URL
        
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                await client.post(url, json=payload, headers=headers)
        except Exception as e:
            logger.error(f"Backend Request Failed: {e} | Payload: {payload.get('conversation_id')}")

    async def increment_and_check_limit(self, user_id: str) -> bool:
        if not user_id: return True 

        try:
            r = await self._get_redis()
            key = f"pending_count:{user_id}"
            
            current_count = await r.incr(key)
            
            if current_count > settings.MAX_PENDING_REQUESTS: 
                logger.warning(f"SPAM BLOCK: User {user_id} has {current_count} pending requests.")
                await r.decr(key) 
                return False
            
            await r.expire(key, 3600) 
            
            logger.info(f"User {user_id} pending count: {current_count}")
            return True

        except Exception as e:
            logger.error(f"Redis Error in limit check: {e}")
            return True 

    async def decrement_user_counter(self, user_id: str):
        if not user_id: return
        try:
            r = await self._get_redis()
            key = f"pending_count:{user_id}"
            
            new_val = await r.decr(key)
            
            if new_val < 0:
                await r.set(key, 0)
                
            logger.info(f"User {user_id} count decremented. New val: {max(0, new_val)}")
        except Exception as e:
            logger.error(f"Failed to decrement counter for {user_id}: {e}")

    async def ask(self, query: str, conversation_id: str, platform: str, user_id: str) -> str:
        is_allowed = await self.increment_and_check_limit(user_id)
        if not is_allowed:
            return "blocked"

        start_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        safe_conv_id = conversation_id or ""

        payload = {
            "query": query,
            "platform": platform,
            "platform_unique_id": user_id,
            "conversation_id": safe_conv_id,
            "start_timestamp": start_timestamp 
        }

        try:
            r = await self._get_redis()
            await r.rpush(settings.REDIS_QUEUE_KEY, json.dumps(payload))
            logger.info(f"QUEUED: {safe_conv_id}")
            return "queued"
        except Exception as e:
            logger.error(f"Failed to enqueue message: {e}")
            await self.decrement_user_counter(user_id)
            return "error"

    async def worker(self, worker_id: int):
        logger.info(f"Redis Worker {worker_id} started...")
        r = await self._get_redis()
        
        while True:
            try:
                # Blocks until item available
                result = await r.blpop(settings.REDIS_QUEUE_KEY, timeout=5)
                
                if result:
                    _, data_json = result
                    payload = json.loads(data_json)
                    logger.debug(f"Worker {worker_id} processing: {payload.get('conversation_id')}")
                    
                    # Fire to Backend AI
                    await self._fire_request(payload)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} Error: {e}")
                await asyncio.sleep(1)