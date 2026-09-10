from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://hookrelay:hookrelay@database:5432/hookrelay"
    jwt_secret: str = Field(default="local-demo-hookrelay-replace-before-deployment", min_length=32)
    token_minutes: int = Field(default=60, ge=1, le=1440)
    testing: bool = False
    worker_interval: float = Field(default=0.5, ge=0.05, le=60)
    amqp_url: str = "amqp://hookrelay:hookrelay@rabbitmq:5672/"
    encryption_key: str = "MD9gxAdH_kBCHi-DxCbhfy0rla-OSZciWC9TIWlZg50="
    allowed_insecure_hosts: list[str] = []
    request_timeout_seconds: float = Field(default=3, ge=0.1, le=30)
    lease_seconds: float = Field(default=10, ge=1, le=120)
    retry_base_seconds: float = Field(default=1, ge=0.1, le=60)
    max_retry_seconds: float = Field(default=60, ge=1, le=600)
    max_attempts: int = Field(default=4, ge=1, le=20)
    demo_receiver_token: str = "local-receiver-control"


settings = Settings()
