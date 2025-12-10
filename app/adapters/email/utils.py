import re
from html import unescape

REGEX_MULTIPLE_NEWLINES = r'\n{3,}'

def strip_html(html: str) -> str:
    if not html: return ""
    
    html = re.sub(r'<hr\s*/?>', '\n__\n', html, flags=re.IGNORECASE)
    html = re.sub(r'<script[^>]>.?</script>', '', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<style[^>]>.?</style>', '', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'</(p|div|br|li|h[1-6]|tr)>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'<[^>]+>', ' ', html)
    html = unescape(html)
    return html.strip()

def strip_quoted_sections(text: str) -> str:
    if not text: return ""
    
    patterns = [
        r'(?:\r\n|\n|^)?\s*On\s+.*(?:at|pukul)\s+.*(?:wrote|menulis):?\s*[\s\S]*',
        r'(?:\r\n|\n|^)?\s*On\s+.*(?:wrote|menulis):?\s*[\s\S]*',
        r'(?:\r\n|\n|^)?\s*Pada\s+.*menulis:\s*[\s\S]*',
        r'(?:\r\n|\n|^)?\s*From:\s*.*\n?Sent:\s*.*\n?To:\s*.*[\s\S]*',
        r'(?:\r\n|\n|^)?\s*Dari:\s*.*\n?Kirim:\s*.*\n?Kepada:\s*.*[\s\S]*',
        r'\n\s*_{3,}[\s\S]*',         
        r'\n\s*-{3,}\s*Original Message\s*-{3,}[\s\S]*', 
        r'\n\s*>[\s\S]*',
    ]
    
    for pattern in patterns:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)
    
    return text.strip()

def sanitize_email_body(text_plain: str, html: str, max_chars: int = 6000) -> str:
    body = text_plain.strip() if text_plain else strip_html(html)
    
    body = strip_quoted_sections(body)
    body = re.sub(r'\s{2,}', ' ', body)
    body = re.sub(REGEX_MULTIPLE_NEWLINES, '\n\n', body)
    
    return body[:max_chars].strip()