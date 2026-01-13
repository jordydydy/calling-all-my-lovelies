import os
from typing import Optional, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    
    # App Settings
    APP_NAME: str = "Multikarnal Orchestrator"
    LOG_LEVEL: str = "INFO"
    ENABLE_BACKGROUND_WORKER: bool = True 
    X_API_KEY: Optional[str] = None 
    BACKEND_CONCURRENCY_LIMIT: int = 5 

    # Backend API Configuration
    BACKEND_API_BASE_URL: str
    BACKEND_API_KEY: str
    
    # Feature Flags
    EMAIL_POLL_INTERVAL_SECONDS: int = 15
    MAX_INPUT_CHARS: int = 6000

    # Redis Configuration
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_DB: int
    REDIS_QUEUE_KEY: str
    
    MAX_PENDING_REQUESTS: int

    # Database
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASS: Optional[str] = None

    # Social Media Credentials
    INSTAGRAM_PAGE_ACCESS_TOKEN: Optional[str] = None
    INSTAGRAM_CHATBOT_ID: Optional[str] = None
    INSTAGRAM_VERIFY_TOKEN: Optional[str] = None

    WHATSAPP_ACCESS_TOKEN: Optional[str] = None
    WHATSAPP_PHONE_NUMBER_ID: Optional[str] = None
    WHATSAPP_VERIFY_TOKEN: Optional[str] = None

    # Email Settings
    EMAIL_PROVIDER: Literal["gmail", "azure_oauth2", "unknown"] = "unknown"
    EMAIL_HOST: str
    EMAIL_PORT: int = 587
    EMAIL_USER: Optional[str] = None
    EMAIL_PASS: Optional[str] = None
    
    # Azure OAuth2
    AZURE_CLIENT_ID: Optional[str] = None
    AZURE_CLIENT_SECRET: Optional[str] = None
    AZURE_TENANT_ID: Optional[str] = None
    AZURE_EMAIL_USER: Optional[str] = None

    @property
    def BACKEND_ASK_URL(self) -> str:
        base = self.BACKEND_API_BASE_URL.rstrip("/")
        return f"{base}/api/chat/multichannel/ask"
    
    @property
    def BACKEND_FEEDBACK_URL(self) -> str:
        base = self.BACKEND_API_BASE_URL.rstrip("/")
        return f"{base}/api/chat/multichannel/feedback"
    
    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True, extra="ignore")

settings = Settings()