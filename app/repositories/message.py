from typing import Optional, Dict
from app.repositories.base import Database
import logging

logger = logging.getLogger("repo.message")

class MessageRepository:
    def is_processed(self, message_id: str, platform: str) -> bool:
        """
        Mengecek apakah pesan sudah diproses menggunakan Database Locking.
        Return:
            True  -> Pesan SUDAH ada (Duplikat / Sedang diproses) -> SKIP
            False -> Pesan BARU (Berhasil dikunci oleh worker ini) -> PROSES
        """
        try:
            with Database.get_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. Pastikan tabel ada
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS bkpm.processed_messages (
                            message_id TEXT NOT NULL,
                            platform TEXT NOT NULL,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (message_id, platform)
                        );
                    """)
                    
                    # 2. ATOMIC LOCK: Coba Insert
                    # Jika ID sudah ada, database akan melempar sinyal konflik (rowcount 0).
                    # Tidak ada dua worker yang bisa sukses insert ID yang sama bersamaan.
                    cursor.execute(
                        """
                        INSERT INTO bkpm.processed_messages (message_id, platform)
                        VALUES (%s, %s)
                        ON CONFLICT (message_id, platform) DO NOTHING
                        """,
                        (message_id, platform)
                    )
                    
                    # Jika rowcount > 0: Berhasil Insert -> Ini pesan baru -> Return False (Belum diproses)
                    # Jika rowcount = 0: Gagal Insert -> Sudah ada -> Return True (Sudah diproses)
                    is_new_entry = cursor.rowcount > 0
                    conn.commit()
                    
                    return not is_new_entry 

        except Exception as e:
            # [FAIL-SAFE]
            # Jika DB error (koneksi putus dll), kita asumsikan True (Sudah diproses)
            # agar tidak terjadi spamming / double reply sampai DB pulih.
            logger.error(f"DB Lock Error for {message_id}: {e}")
            return True 

    def get_conversation_by_thread(self, thread_key: str) -> Optional[str]:
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
                            thread_key = EXCLUDED.thread_key
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
                        "SELECT subject, in_reply_to, \"references\", thread_key FROM bkpm.email_metadata WHERE conversation_id = %s LIMIT 1", 
                        (conversation_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        return {"subject": row[0], "in_reply_to": row[1], "references": row[2], "thread_key": row[3]}
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