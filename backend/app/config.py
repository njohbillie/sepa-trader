from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    redis_url: str = "redis://redis:6379"

    alpaca_paper_key: str
    alpaca_paper_secret: str
    alpaca_live_key: str = ""
    alpaca_live_secret: str = ""

    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    secret_key: str = "dev-secret-change-me"

    # Auth bootstrap — admin credentials on first startup
    admin_email:    str = "admin@sepa.local"
    admin_password: str = ""   # auto-generated if blank; printed to logs once

    # CORS — comma-separated list of allowed frontend origins
    allowed_origins: str = "http://localhost,http://localhost:5173,http://localhost:3000"

    # Set to true in production (HTTPS only) to mark auth cookies Secure
    secure_cookies: bool = False

    class Config:
        env_file = ".env"


settings = Settings()
