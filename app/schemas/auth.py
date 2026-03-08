from pydantic import BaseModel


class CurrentUser(BaseModel):
    id: str | None = None
    firebase_uid: str
    email: str | None = None
