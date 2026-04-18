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

    class Config:
        env_file = ".env"


settings = Settings()
