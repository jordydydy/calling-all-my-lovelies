import imaplib
import email
import time
import asyncio
import logging
import msal
import requests
from email.header import decode_header
from typing import Dict, Any, Optional

from app.core.config import settings
from app.adapters.email.utils import sanitize_email_body
from app.repositories.message import MessageRepository
from app.api.dependencies import get_orchestrator
from app.schemas.models import IncomingMessage

logger = logging.getLogger("email.listener")
repo = MessageRepository()

_token_cache: Dict[str, Any] = {}

def get_graph_token() -> Optional[str]:
    global _token_cache
    if _token_cache and _token_cache.get("expires_at", 0) > time.time() + 60:
        return _token_cache.get("access_token")

    if not all([settings.AZURE_CLIENT_ID, settings.AZURE_CLIENT_SECRET, settings.AZURE_TENANT_ID]):
        logger.error("Azure credentials not fully configured for Listener.")
        return None

    try:
        app = msal.ConfidentialClientApplication(
            settings.AZURE_CLIENT_ID,
            authority=f"https://login.microsoftonline.com/{settings.AZURE_TENANT_ID}",
            client_credential=settings.AZURE_CLIENT_SECRET,
        )
        result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        if "access_token" in result:
            _token_cache = {
                "access_token": result["access_token"],
                "expires_at": time.time() + result.get("expires_in", 3500)
            }
            return result["access_token"]
        return None
    except Exception as e:
        logger.error(f"Azure Auth Exception: {e}")
        return None

def decode_str(header_val):
    if not header_val: return ""
    decoded_list = decode_header(header_val)
    text = ""
    for content, encoding in decoded_list:
        if isinstance(content, bytes):
            text += content.decode(encoding or "utf-8", errors="ignore")
        else:
            text += str(content)
    return text

def process_single_email(sender_email, body, metadata: dict):
    if "mailer-daemon" in sender_email.lower() or "noreply" in sender_email.lower():
        return

    msg = IncomingMessage(
        platform_unique_id=sender_email,
        query=body,
        platform="email",
        metadata=metadata
    )
    
    try:
        orchestrator = get_orchestrator()
        
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        loop.run_until_complete(orchestrator.process_message(msg))
        logger.info(f"Email processed internally: {sender_email}")
    except Exception as err:
        logger.error(f"Failed internal process: {err}")

def _extract_graph_body(msg):
    body_content = msg.get("body", {}).get("content", "")
    body_type = msg.get("body", {}).get("contentType", "Text")
    return sanitize_email_body(None, body_content) if body_type.lower() == "html" else sanitize_email_body(body_content, None)

def _process_graph_message(user_id, msg, token):
    graph_id = msg.get("id")
    azure_conv_id = msg.get("conversationId")
    
    if not graph_id or repo.is_processed(graph_id, "email"):
        return

    clean_body = _extract_graph_body(msg)
    if not clean_body: return

    sender_info = msg.get("from", {}).get("emailAddress", {})
    
    metadata = {
        "subject": msg.get("subject", "No Subject"),
        "sender_name": sender_info.get("name", ""),
        "graph_message_id": graph_id,
        "conversation_id": azure_conv_id
    }

    process_single_email(sender_info.get("address", ""), clean_body, metadata)
    _mark_graph_read(user_id, graph_id, token)

def _poll_graph_api():
    token = get_graph_token()
    if not token: return
    user_id = settings.AZURE_EMAIL_USER
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/inbox/messages"
    params = {"$filter": "isRead eq false", "$top": 10}
    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=20)
        if resp.status_code == 200:
            for msg in resp.json().get("value", []):
                _process_graph_message(user_id, msg, token)
    except Exception as e:
        logger.error(f"Graph Polling Error: {e}")

def _mark_graph_read(user_id, message_id, token):
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}"
    try:
        requests.patch(url, json={"isRead": True}, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, timeout=5)
    except Exception: pass

def _determine_thread_key(references: str, in_reply_to: str, msg_id: str) -> str:
    if references:
        return references.split()[0].strip()
    if in_reply_to:
        return in_reply_to.strip()
    return msg_id

def _extract_email_parts(msg) -> tuple:
    text_plain = ""
    html = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                text_plain = part.get_payload(decode=True).decode(errors='ignore')
            elif content_type == "text/html":
                html = part.get_payload(decode=True).decode(errors='ignore')
    else:
        text_plain = msg.get_payload(decode=True).decode(errors='ignore')
    
    return text_plain, html

def _extract_sender_info(sender: str) -> tuple:
    if '<' in sender:
        email_addr = sender.split('<')[-1].replace('>', '').strip()
        sender_name = sender.split('<')[0].strip()
    else:
        email_addr = sender
        sender_name = sender
    
    return email_addr, sender_name

def _process_imap_message(mail, e_id):
    try:
        _, msg_data = mail.fetch(e_id, '(RFC822)')
        msg = email.message_from_bytes(msg_data[0][1])
        msg_id = msg.get("Message-ID", "").strip()
        
        if not msg_id or repo.is_processed(msg_id, "email"):
            return
        
        text_plain, html = _extract_email_parts(msg)
        clean_body = sanitize_email_body(text_plain, html)
        if not clean_body:
            return

        sender = decode_str(msg.get("From"))
        email_addr, sender_name = _extract_sender_info(sender)
        
        references = msg.get("References", "")
        in_reply_to = msg.get("In-Reply-To", "")
        thread_key = _determine_thread_key(references, in_reply_to, msg_id)

        metadata = {
            "subject": decode_str(msg.get("Subject")),
            "sender_name": sender_name,
            "message_id": msg_id,
            "thread_key": thread_key,
            "in_reply_to": in_reply_to,
            "references": references
        }
        
        process_single_email(email_addr, clean_body, metadata)
    except Exception as e:
        logger.error(f"IMAP Error: {e}")

def _poll_imap():
    try:
        mail = imaplib.IMAP4_SSL(settings.EMAIL_HOST, settings.EMAIL_PORT)
        mail.login(settings.EMAIL_USER, settings.EMAIL_PASS)
        mail.select("INBOX")
        _, messages = mail.search(None, 'UNSEEN')
        if messages[0]:
            for e_id in messages[0].split():
                _process_imap_message(mail, e_id)
        mail.close()
        mail.logout()
    except Exception as e:
        logger.error(f"IMAP Loop Error: {e}")

def start_email_listener():
    if not settings.EMAIL_USER: return
    logger.info(f"Starting Email Listener for {settings.EMAIL_PROVIDER}...")
    while True:
        if settings.EMAIL_PROVIDER == "azure_oauth2": _poll_graph_api()
        else: _poll_imap()
        time.sleep(settings.EMAIL_POLL_INTERVAL_SECONDS)