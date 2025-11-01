# models.py
from sqlalchemy import Column, Integer, BigInteger, Text, DateTime, JSON, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String
from db import Base

Base = declarative_base()

class District(Base):
    __tablename__ = "districts"
    id = Column(Integer, primary_key=True)
    state_name = Column(Text, nullable=False)
    district_name = Column(Text, nullable=False)
    district_code = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

class MgnregaMonthly(Base):
    __tablename__ = "mgnrega_monthly"
    id = Column(Integer, primary_key=True)
    district_id = Column(Integer, ForeignKey("districts.id"), nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    persons_benefitted = Column(BigInteger, default=0)
    person_days = Column(BigInteger, default=0)
    wages_paid = Column(BigInteger, default=0)
    households_worked = Column(BigInteger, default=0)
    source_date = Column(DateTime(timezone=True), server_default=func.now())
    raw_json = Column(JSON)
    __table_args__ = (UniqueConstraint('district_id','year','month', name='uq_district_month'),)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False)
    password = Column(String(200), nullable=False)
