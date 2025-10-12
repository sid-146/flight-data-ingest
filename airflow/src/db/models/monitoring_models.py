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
    Sequence,
    Text,
)

from sqlalchemy.orm import relationship

from src.db.config import BASE


# class TestTable(BASE):
#     __tablename__ = "testTable"
#     __table_args__ = {"schema": "monitoring"}
#     id: str = Column(String, nullable=False, primary_key=True)
#     dag_name: str = Column(String, nullable=False)


class ProcessRunLog(BASE):
    __tablename__ = "process_run_log"
    __table_args__ = {"schema": "monitoring"}

    id: int = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        server_default=Sequence("process_run_log_id_seq").next_value(),
    )
    dag_name: str = Column(String, nullable=False)
    starttime: datetime = Column(DateTime, nullable=False, default=func.now())
    status: str = Column(String)
    task: str = Column(String, nullable=True)
    errors: str = Column(Text, nullable=True)

    files = relationship("IngestionFileLog", back_populates="process_run")


class IngestionFileLog(BASE):
    __tablename__ = "ingestion_file_log"
    __table_args__ = {"schema": "monitoring"}

    id: int = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        server_default=Sequence("ingestion_file_log_id_seq").next_value(),
    )
    run_id: int = Column(BigInteger, ForeignKey("monitoring.process_run_log.id"))
    no_records: int = Column(Integer)
    s3_key: str = Column(String)
    is_processed: str = Column(String)
    data_type: str = Column(String)

    process_run = relationship("ProcessRunLog", back_populates="files")
