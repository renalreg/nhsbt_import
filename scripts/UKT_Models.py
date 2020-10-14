from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date

Base = declarative_base()

# http://docs.sqlalchemy.org/en/latest/dialects/oracle.html#identifier-casing


class UKT_Patient(Base):
    __tablename__ = 'ukt_patients'

    # Note - SQLAlchemy sends 'proper case' items to Oracle in speech marks implying Case Sensitivity - which then doesn't match.
    UKTSSA_No = Column('uktssa_no', Integer, primary_key=True, doc='Test', info={'Test': 'Blick'}, autoincrement=False)
    Surname = Column('surname', String)
    Forename = Column('forename', String)
    Sex = Column('sex', String)
    Post_Code = Column('post_code', String)
    New_NHS_No = Column('new_nhs_no', Integer)
    RR_No = Column('rr_no', Integer)
    UKT_Date_Death = Column('ukt_date_death', Date)
    UKT_Date_Birth = Column('ukt_date_birth', Date)


class UKT_Transplant(Base):
    __tablename__ = 'ukt_transplants'

    Transplant_ID = Column('transplant_id', Integer)
    UKTSSA_No = Column('uktssa_no', Integer)
    Transplant_Date = Column('transplant_date', Date)
    Transplant_Type = Column('transplant_type', String)
    Transplant_Organ = Column('transplant_organ', String)
    Transplant_Unit = Column('transplant_unit', String)
    UKT_Fail_Date = Column('ukt_fail_date', Date)
    RR_No = Column('rr_no', Integer)
    Registration_ID = Column('registration_id', String, primary_key=True)
    Registration_Date = Column('registration_date', Date)
    Registration_Date_Type = Column('registration_date_type', String)
    Registration_End_Date = Column('registration_end_date', Date)
    Registration_End_Status = Column('registration_end_status', String)
    Transplant_Consideration = Column('transplant_consideration', String)
    Transplant_Dialysis = Column('transplant_dialysis', String)
    Transplant_Relationship = Column('transplant_relationship', String)
    Transplant_Sex = Column('transplant_sex', String)
    Cause_Of_Failure = Column('cause_of_failure', String)
    Cause_Of_Failure_Text = Column('cause_of_failure_text', String)
    CIT_Mins = Column('cit_mins', String)
    HLA_Mismatch = Column('hla_mismatch', String)
    UKT_SUSPENSION = Column('UKT_SUSPENSION', String)
