from pydantic import BaseModel

class Query(BaseModel):
    date: str
    symbol: str  # allow multiple user sessions
