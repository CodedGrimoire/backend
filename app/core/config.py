from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine.url import make_url


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="allow"
    )

    # ---- Core Settings ----
    database_url: str | None = Field(None, alias="DATABASE_URL")
    firebase_project_id: str | None = Field(None, alias="FIREBASE_PROJECT_ID")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    # ---- Development Mode ----
    dev_mode: bool = Field(True, alias="DEV_MODE")
    dev_user_id: str = Field(
        "00000000-0000-0000-0000-000000000001",
        alias="DEV_USER_ID"
    )

    # ---- LLM Configuration (Groq only) ----
    groq_api_key: str = Field(..., alias="GROQ_API_KEY")
    groq_model: str = Field("llama-3.1-70b-versatile", alias="GROQ_MODEL")

    # provider is now fixed
    llm_provider: str = "groq"

    def model_post_init(self, __context) -> None:
        try:
            if not self.database_url:
                return

            url = make_url(self.database_url)

            # convert postgres → asyncpg
            if url.drivername in {"postgresql", "postgres", "psycopg2"}:
                url = url.set(drivername="postgresql+asyncpg")

            query = dict(url.query) if url.query else {}

            # remove unsupported params
            query.pop("channel_binding", None)
            query.pop("sslmode", None)

            url = url.set(query=query)

            object.__setattr__(
                self,
                "database_url",
                url.render_as_string(hide_password=False)
            )

        except Exception:
            pass


settings = Settings()