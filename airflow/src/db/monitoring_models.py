from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Boolean,
    Float,
    BigInteger,
)

# from sqlalchemy.orm import relationship

from .config import BASE


class TestTable(BASE):
    __tablename__ = "testTable"
    __table_args__ = {"schema": "monitoring"}
    id: str = Column(String, nullable=False, primary_key=True)
