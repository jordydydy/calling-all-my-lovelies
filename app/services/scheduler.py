import asyncio
import logging
from app.api.dependencies import get_orchestrator
from app.repositories.conversation import ConversationRepository

logger = logging.getLogger("service.scheduler")

async def run_scheduler():
    logger.info("Session Timeout Scheduler Started...")
    repo_conv = ConversationRepository()    
    await asyncio.sleep(5)

    while True:
        try:
            orchestrator = get_orchestrator()
            stale_sessions = repo_conv.get_stale_sessions(minutes=5)
    
            if stale_sessions:
                logger.info(f"🔍 Found {len(stale_sessions)} stale sessions.")

            for session in stale_sessions:
                conv_id, platform, user_id = session
                
                await orchestrator.timeout_session(conv_id, platform, user_id)
                
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Scheduler Error: {e}")
        
        await asyncio.sleep(60)