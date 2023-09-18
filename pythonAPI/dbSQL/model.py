from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID

from .database import Base
import uuid


class PreprocessEntry(Base):
    __tablename__ = "entries"
    id = Column(Text(length=36), default=lambda: str(uuid.uuid4()), primary_key=True, index=True)
    message = Column(String, default="Starting")
    status = Column(String, default="starting")

    def __str__(self):
        return "Process: [id : {}, status: {}, message: {}]".format(self.id,self.status,self.message)

    def __repr__(self):
        return "Process: [id : {}, status: {}, message: {}]".format(self.id, self.status, self.message)