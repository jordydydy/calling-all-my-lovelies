import os
from typing import Optional, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    
    # App Settings
    APP_NAME: str = "Multikarnal Orchestrator"
    LOG_LEVEL: str = "INFO"
    
    # [BARU] Flag untuk mengontrol Email Listener & Scheduler
    ENABLE_BACKGROUND_WORKER: bool = True 

    # Chatbot / Backend
    CHATBOT_URL: Optional[str] = None
    CHATBOT_API_KEY: Optional[str] = None
    CHATBOT_TIMEOUT_SECONDS: int = 30
    
    CORE_API_BASE_URL: str
    CORE_API_KEY: Optional[str] = None
    
    # Feature Flags
    AUTO_REPLY_TO_PLATFORM: bool = False
    EMAIL_POLL_INTERVAL_SECONDS: int
    MAX_INPUT_CHARS: int

    # Database
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASS: str

    # Social Media Credentials
    INSTAGRAM_PAGE_ACCESS_TOKEN: Optional[str] = None
    INSTAGRAM_CHATBOT_ID: Optional[str] = None
    INSTAGRAM_VERIFY_TOKEN: Optional[str] = None

    WHATSAPP_ACCESS_TOKEN: Optional[str] = None
    WHATSAPP_PHONE_NUMBER_ID: Optional[str] = None
    WHATSAPP_VERIFY_TOKEN: Optional[str] = None

    # Email Settings
    EMAIL_PROVIDER: Literal["gmail", "azure_oauth2", "unknown"] = "unknown"
    EMAIL_HOST: str = "imap.gmail.com"
    EMAIL_PORT: int = 993
    EMAIL_USER: Optional[str] = None
    EMAIL_PASS: Optional[str] = None
    
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    
    # Azure OAuth2
    AZURE_CLIENT_ID: Optional[str] = None
    AZURE_CLIENT_SECRET: Optional[str] = None
    AZURE_TENANT_ID: Optional[str] = None
    AZURE_EMAIL_USER: Optional[str] = None

    @property
    def FEEDBACK_API_URL(self) -> str:
        base = self.CORE_API_BASE_URL.rstrip("/")
        return f"{base}/api/chat/multichannel/feedback"
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = Settings()