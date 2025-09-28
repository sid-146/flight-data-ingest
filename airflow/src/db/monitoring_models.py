from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Boolean,
    Float,
    BigInteger,
    DateTime,
    func,
)

# from sqlalchemy.orm import relationship

from .config import BASE


# class TestTable(BASE):
#     __tablename__ = "testTable"
#     __table_args__ = {"schema": "monitoring"}
#     id: str = Column(String, nullable=False, primary_key=True)
#     dag_name: str = Column(String, nullable=False)


class ProcessRunLog(BASE):
    __tablename__ = "process_run_log"
    __table_args__ = {"schema": "monitoring"}

    id: int = Column(BigInteger, nullable=False, primary_key=True, autoincrement=True)
    dag_name: str = Column(String, nullable=False)
    starttime: datetime = Column(DateTime, nullable=False, default=func.now())
    status: str = Column(String)
