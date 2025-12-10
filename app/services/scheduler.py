import asyncio
import logging
from app.api.dependencies import get_orchestrator
from app.repositories.conversation import ConversationRepository

logger = logging.getLogger("service.scheduler")

async def run_scheduler():
    """
    Background task untuk menutup sesi idle.
    [MODE TESTING CEPAT]
    """
    logger.info("⏳ Session Timeout Scheduler Started (FAST MODE: 10 Mins Idle Check)...")
    
    repo_conv = ConversationRepository()
    
    # Delay awal
    await asyncio.sleep(5)

    while True:
        try:
            orchestrator = get_orchestrator()
            
            stale_sessions = repo_conv.get_stale_sessions(minutes=10)
            
            if stale_sessions:
                logger.info(f"🔍 Found {len(stale_sessions)} stale sessions. Closing them...")

            for session in stale_sessions:
                conv_id, platform, user_id = session
                
                # Tutup sesi
                await orchestrator.timeout_session(conv_id, platform, user_id)
                
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Scheduler Error: {e}")
        
        # [TESTING] Cek database setiap 10 detik (agar cepat ketahuan hasilnya)
        await asyncio.sleep(60)