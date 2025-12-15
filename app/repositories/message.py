from typing import Optional, Dict
from app.repositories.base import Database
import logging

logger = logging.getLogger("repo.message")

class MessageRepository:
    def is_processed(self, message_id: str, platform: str) -> bool:
        """
        Check if message already processed using DB lock
        For Azure: message_id = graph_id
        For IMAP: message_id = Message-ID header
        """
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO bkpm.processed_messages (message_id, platform)
                        VALUES (%s, %s)
                        ON CONFLICT (message_id, platform) DO NOTHING
                        """,
                        (message_id, platform)
                    )
                    is_new_entry = cursor.rowcount > 0
                    conn.commit()
                    
                    return not is_new_entry 
                
        except Exception as e:
            logger.error(f"DB Lock Error for {message_id}: {e}")
            return True 

    def get_conversation_by_thread(self, thread_key: str) -> Optional[str]:
        """Get conversation by IMAP thread_key (for Gmail/IMAP)"""
        if not thread_key: return None
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT conversation_id FROM bkpm.email_metadata WHERE thread_key = %s LIMIT 1", 
                        (thread_key,)
                    )
                    row = cursor.fetchone()
                    return str(row[0]) if row else None
        except Exception as e:
            logger.error(f"Failed to get conversation by thread: {e}")
            return None

    def get_conversation_by_azure_thread(self, azure_conversation_id: str) -> Optional[str]:
        """Get conversation by Azure conversationId (for Office365)"""
        if not azure_conversation_id: return None
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Use thread_key column to store azure conversationId
                    cursor.execute(
                        "SELECT conversation_id FROM bkpm.email_metadata WHERE thread_key = %s LIMIT 1", 
                        (azure_conversation_id,)
                    )
                    row = cursor.fetchone()
                    return str(row[0]) if row else None
        except Exception as e:
            logger.error(f"Failed to get conversation by Azure thread: {e}")
            return None

    def save_email_metadata(self, conversation_id: str, subject: str, in_reply_to: str, references: str, thread_key: str):
        """
        Save email metadata for threading
        For Azure: in_reply_to = graph_message_id, thread_key = azure conversationId
        For IMAP: in_reply_to = In-Reply-To header, thread_key = first message in thread
        """
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO bkpm.email_metadata (conversation_id, subject, in_reply_to, "references", thread_key)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (conversation_id) 
                        DO UPDATE SET
                            subject = EXCLUDED.subject,
                            in_reply_to = EXCLUDED.in_reply_to,
                            "references" = EXCLUDED."references",
                            thread_key = EXCLUDED.thread_key
                        """,
                        (conversation_id, subject, in_reply_to, references, thread_key)
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to save email metadata: {e}")

    def get_email_metadata(self, conversation_id: str) -> Optional[Dict[str, str]]:
        """
        Get email metadata for reply
        Returns: subject, in_reply_to (graph_id for Azure), references, thread_key
        """
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT subject, in_reply_to, \"references\", thread_key FROM bkpm.email_metadata WHERE conversation_id = %s LIMIT 1", 
                        (conversation_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        return {
                            "subject": row[0], 
                            "in_reply_to": row[1],  
                            "graph_message_id": row[1], 
                            "references": row[2], 
                            "thread_key": row[3]  
                        }
            return None
        except Exception as e:
            logger.error(f"Failed to get email metadata: {e}")
            return None

    def get_latest_answer_id(self, conversation_id: str) -> Optional[int]:
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM bkpm.chat_history WHERE session_id = %s ORDER BY created_at DESC LIMIT 1", (conversation_id,))
                    row = cursor.fetchone()
                    return int(row[0]) if row else None
        except Exception as e:
            logger.warning(f"No answer history found for {conversation_id}: {e}")
            return None