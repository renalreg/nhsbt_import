from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date

Base = declarative_base()


class RR_Patient(Base):

    __tablename__ = 'PATIENTS'

    # Note - SQLAlchemy sends 'proper case' items to Oracle in speech
    # marks implying Case Sensitivity - which then doens't match.
    RR_No = Column('RR_NO', Integer, primary_key=True)
    Surname = Column('SURNAME', String)
    Forename = Column('FORENAME', String)
    Sex = Column('SEX', String)
    New_NHS_No = Column('NEW_NHS_NO', Integer)
    Date_Death = Column('DATE_DEATH', Date)
    Date_Birth = Column('DATE_BIRTH', Date)

    # There are additional fields to be added but I don't need these yet


class RR_Deleted_Patient(Base):

    __tablename__ = "DELETED_PATIENTS"

    # Note - SQLAlchemy sends 'proper case' items to Oracle in speech
    # marks implying Case Sensitivity - which then doens't match.
    RR_No = Column('RR_NO', Integer, primary_key=True)
    Surname = Column('SURNAME', String)
    Forename = Column('FORENAME', String)
    Sex = Column('SEX', String)
    New_NHS_No = Column('NEW_NHS_NO', Integer)
    Date_Death = Column('DATE_DEATH', Date)
    Date_Birth = Column('DATE_BIRTH', Date)

    # There are additional fields to be added but I don't need these yet
