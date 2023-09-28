from typing import Optional

from pydantic import BaseModel


class PreprocessEntryBase(BaseModel):
    id: str
    output: Optional[str]
    error: Optional[str]
    status: str

    class Config:
        orm_mode = True


class PreprocessEntryCreate(PreprocessEntryBase):
    pass
