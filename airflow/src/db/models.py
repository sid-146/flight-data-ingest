from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean
from sqlalchemy.orm import relationship

from .config import BASE


class Flights(BASE):
    __tablename__ = "flights"
    __table_args__ = {"schema": "flight_data"}
    Id: str = Column(String, primary_key=True, nullable=False)
    status: bool = Column(Boolean, nullable=True, comment="Status of current flight.")
