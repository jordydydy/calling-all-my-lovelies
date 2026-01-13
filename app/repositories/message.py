from typing import Optional, Dict
from psycopg import errors
from app.repositories.base import Database
from app.core.exceptions import DatabaseError
import logging

logger = logging.getLogger("repo.message")

class MessageRepository:
    def is_processed(self, message_id: str, platform: str) -> bool:
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO bkpm.processed_messages (message_id, platform)
                            VALUES (%s, %s)
                            """,
                            (message_id, platform)
                        )
                        conn.commit()
                        return False 
                        
                    except errors.UniqueViolation:
                        conn.rollback()
                        return True 
                        
        except Exception as e:
            if "duplicate key" in str(e).lower():
                return True
            logger.error(f"DB Check Error: {e}")
            return True 

    def get_conversation_by_azure_thread(self, azure_conversation_id: str) -> Optional[str]:
        if not azure_conversation_id: return None
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT conversation_id 
                        FROM bkpm.email_metadata 
                        WHERE thread_key = %s 
                        LIMIT 1
                        """, 
                        (azure_conversation_id,)
                    )
                    row = cursor.fetchone()
                    return str(row[0]) if row else None
        except Exception as e:
            logger.error(f"Failed to find Azure thread: {e}")
            return None

    def get_conversation_by_thread(self, thread_key: str) -> Optional[str]:
        return self.get_conversation_by_azure_thread(thread_key)

    def save_email_metadata(self, conversation_id: str, subject: str, in_reply_to: str, references: str, thread_key: str):
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
                            thread_key = EXCLUDED.thread_key,
                            updated_at = NOW()
                        """,
                        (conversation_id, subject, in_reply_to, references, thread_key)
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"Failed to save email metadata: {e}")

    def get_email_metadata(self, conversation_id: str) -> Optional[Dict[str, str]]:
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT subject, in_reply_to, "references", thread_key 
                        FROM bkpm.email_metadata 
                        WHERE conversation_id = %s 
                        LIMIT 1
                        """, 
                        (conversation_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        return {
                            "subject": row[0], 
                            "in_reply_to": row[1],  
                            "graph_message_id": row[1], # Alias untuk Azure
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
        except Exception:
            return None