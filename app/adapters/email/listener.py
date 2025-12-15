import imaplib
import email
import time
import requests
import logging
import msal
from email.header import decode_header
from typing import Dict, Any, Optional

from app.core.config import settings
from app.adapters.email.utils import sanitize_email_body
from app.repositories.message import MessageRepository

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
            logger.info("New Azure OAuth2 token acquired for Listener.")
            return result["access_token"]
        else:
            logger.error(f"Failed to acquire Graph token: {result.get('error_description')}")
            return None
    except Exception as e:
        logger.error(f"Azure Auth Exception in Listener: {e}")
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

def process_single_email(sender_email, sender_name, subject, body, graph_message_id, conversation_id):
    """
    Process email using Azure Graph identifiers
    - graph_message_id: unique per message (for deduplication)
    - conversation_id: groups messages in same thread (for threading)
    """
    if "mailer-daemon" in sender_email.lower() or "noreply" in sender_email.lower():
        return

    payload = {
        "platform_unique_id": sender_email,
        "query": body,
        "platform": "email",
        "metadata": {
            "subject": subject,
            "sender_name": sender_name,
            "graph_message_id": graph_message_id,  # For deduplication
            "conversation_id": conversation_id      # For threading
        }
    }
    
    try:
        api_url = "http://0.0.0.0:9798/api/messages/process" 
        resp = requests.post(api_url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info(f"Email queued: {sender_email} | ConvID: {conversation_id}")
        else:
            logger.error(f"API returned {resp.status_code}: {resp.text}")
    except Exception as req_err:
        logger.error(f"Failed to push email to API: {req_err}")

def _extract_graph_body(msg):
    body_content = msg.get("body", {}).get("content", "")
    body_type = msg.get("body", {}).get("contentType", "Text")
    
    if body_type.lower() == "html":
        return sanitize_email_body(None, body_content)
    return sanitize_email_body(body_content, None)

def _process_graph_message(user_id, msg, token):
    """
    Process Azure Graph API message
    Uses graph_id for deduplication and conversationId for threading
    """
    graph_id = msg.get("id")
    conversation_id = msg.get("conversationId")
    
    if not graph_id:
        logger.warning("Message missing Graph ID, skipping")
        return

    # Check if already processed (DB only, no in-memory cache)
    if repo.is_processed(graph_id, "email"):
        _mark_graph_read(user_id, graph_id, token)
        logger.debug(f"Email already processed: {graph_id}")
        return

    clean_body = _extract_graph_body(msg)
    if not clean_body: 
        _mark_graph_read(user_id, graph_id, token)
        logger.debug(f"Empty body, marking as read: {graph_id}")
        return

    sender_info = msg.get("from", {}).get("emailAddress", {})
    sender_email = sender_info.get("address", "")
    
    if not sender_email:
        _mark_graph_read(user_id, graph_id, token)
        logger.warning(f"No sender email found: {graph_id}")
        return
    
    process_single_email(
        sender_email,
        sender_info.get("name", ""),
        msg.get("subject", "No Subject"),
        clean_body,
        graph_id,
        conversation_id
    )
    
    _mark_graph_read(user_id, graph_id, token)

def _poll_graph_api():
    token = get_graph_token()
    if not token: return

    user_id = settings.AZURE_EMAIL_USER
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/inbox/messages"
    params = {
        "$filter": "isRead eq false",
        "$top": 10,
        "$select": "id,subject,from,body,conversationId,isRead"
    }
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        if resp.status_code != 200:
            logger.error(f"Graph API Error {resp.status_code}: {resp.text}")
            return

        messages = resp.json().get("value", [])
        if messages:
            logger.info(f"Found {len(messages)} new emails via Graph API.")

        for msg in messages:
            try:
                _process_graph_message(user_id, msg, token)
            except Exception as e:
                logger.error(f"Error processing graph message: {e}")

    except Exception as e:
        logger.error(f"Graph Polling Exception: {e}")

def _mark_graph_read(user_id, message_id, token):
    """Mark message as read in Office365"""
    url = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    try:
        requests.patch(url, json={"isRead": True}, headers=headers, timeout=5)
    except Exception as e:
        logger.debug(f"Failed to mark as read: {e}")

# IMAP Functions (kept for Gmail support)
def _fetch_and_parse_imap(mail, e_id):
    fetch_res = mail.fetch(e_id, '(RFC822)')
    if not fetch_res or len(fetch_res) != 2:
        logger.warning(f"Skipping email {e_id}: Fetch returned empty/invalid response.")
        return None
    
    _, msg_data = fetch_res
    if not msg_data or not isinstance(msg_data, list) or not msg_data[0]:
        logger.warning(f"Skipping email {e_id}: Message data is empty/invalid.")
        return None

    return email.message_from_bytes(msg_data[0][1])

def _extract_imap_sender(msg):
    sender = decode_str(msg.get("From"))
    if '<' in sender:
        email_addr = sender.split('<')[-1].replace('>', '').strip()
        name = sender.split('<')[0].strip()
    else:
        email_addr = sender
        name = sender
    return email_addr, name

def _extract_imap_body(msg):
    text_plain, html = "", ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                text_plain = part.get_payload(decode=True).decode(errors='ignore')
            elif ctype == "text/html":
                html = part.get_payload(decode=True).decode(errors='ignore')
    else:
        text_plain = msg.get_payload(decode=True).decode(errors='ignore')
    return sanitize_email_body(text_plain, html)

def _process_imap_message(mail, e_id):
    try:
        msg = _fetch_and_parse_imap(mail, e_id)
        if not msg: return

        msg_id = msg.get("Message-ID", "").strip()
        
        if not msg_id or repo.is_processed(msg_id, "email"):
            return
        
        clean_body = _extract_imap_body(msg)
        if not clean_body: return

        sender_email, sender_name = _extract_imap_sender(msg)
        references = msg.get("References", "")
        in_reply_to = msg.get("In-Reply-To", "")
        
        # For IMAP, use Message-ID as thread key
        thread_key = references.split()[0].strip() if references else (in_reply_to.strip() if in_reply_to else msg_id)

        payload = {
            "platform_unique_id": sender_email,
            "query": clean_body,
            "platform": "email",
            "metadata": {
                "subject": decode_str(msg.get("Subject")),
                "sender_name": sender_name,
                "message_id": msg_id,
                "thread_key": thread_key,
                "in_reply_to": in_reply_to,
                "references": references
            }
        }
        
        try:
            api_url = "http://0.0.0.0:9798/api/messages/process"
            requests.post(api_url, json=payload, timeout=10)
            logger.info(f"IMAP Email queued: {sender_email}")
        except Exception as req_err:
            logger.error(f"Failed to push IMAP email: {req_err}")
    
    except Exception as e_inner:
        logger.error(f"Error processing IMAP email {e_id}: {e_inner}")

def _poll_imap():
    try:
        mail = imaplib.IMAP4_SSL(settings.EMAIL_HOST, settings.EMAIL_PORT)
        mail.login(settings.EMAIL_USER, settings.EMAIL_PASS)
        mail.select("INBOX")
        
        _, messages = mail.search(None, 'UNSEEN')
        if not messages or not messages[0]:
            mail.close()
            mail.logout()
            return

        email_ids = messages[0].split()
        
        if email_ids:
            logger.info(f"Found {len(email_ids)} new emails via IMAP.")
            
        for e_id in email_ids:
            _process_imap_message(mail, e_id)

        mail.close()
        mail.logout()

    except Exception as e:
        logger.error(f"IMAP Loop Error: {e}")

def start_email_listener():
    if not settings.EMAIL_USER:
        logger.warning("Email credentials not set. Listener stopped.")
        return

    logger.info(f"Starting Email Listener for provider: {settings.EMAIL_PROVIDER}...")
    
    while True:
        if settings.EMAIL_PROVIDER == "azure_oauth2":
            _poll_graph_api()
        else:
            _poll_imap()
        time.sleep(settings.EMAIL_POLL_INTERVAL_SECONDS)