import argparse
import logging
import logging.config
import os

import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from ukrr_models.nhsbt_models import UKT_Patient, UKT_Transplant


def args_parse(argv=None):
    parser = argparse.ArgumentParser(description="nhsbt_import")
    parser.add_argument("-if", "--input_file", type=str, help="Specify Input File")
    return parser.parse_args(argv), parser.format_help()


def create_df(name, columns):
    return pd.DataFrame(columns=columns[name])


def create_incoming_patient(row):
    return UKT_Patient(
        uktssa_no=row["UKTR_ID"],
        surname=row["UKTR_RSURNAME"],
        forename=row["UKTR_RFORENAME"],
        sex=row["UKTR_RSEX"],
        post_code=row["UKTR_RPOSTCODE"],
        new_nhs_no=row["UKTR_RNHS_NO"],
        chi_no=row["UKTR_RCHI_NO_NI"],
        hsc_no=row["UKTR_RCHI_NO_SCOT"],
        rr_no=None,
        ukt_date_death=row["UKTR_DDATE"],
        ukt_date_birth=row["UKTR_RDOB"],
    )


def create_incoming_transplant(row, transplant_counter):
    return UKT_Transplant(
        Transplant_ID=row[f"uktr_tx_id{transplant_counter}"],
        UKTSSA_No=row["UKTR_ID"],
        Transplant_Date=row[f"uktr_txdate{transplant_counter}"],
        Transplant_Type=row[f"uktr_dgrp{transplant_counter}"],
        Transplant_Organ=row[f"uktr_tx_type{transplant_counter}"],
        Transplant_Unit=row[f"uktr_tx_unit{transplant_counter}"],
        UKT_Fail_Date=row[f"uktr_faildate1{transplant_counter}"],
        RR_No=None,
        Registration_ID=f'{row["UKTR_ID"]}_{transplant_counter}',
        Registration_Date=row[f"uktr_date_on{transplant_counter}"],
        Registration_Date_Type=row[f"uktr_list_status{transplant_counter}"],
        Registration_End_Date=row[f"uktr_removal_date{transplant_counter}"],
        Registration_End_Status=row[f"uktr_endstat{transplant_counter}"],
        Transplant_Consideration=row[f"uktr_tx_list{transplant_counter}"],
        Transplant_Dialysis=row[f"uktr_dial_at_tx{transplant_counter}"],
        Transplant_Relationship=row[f"uktr_relationship{transplant_counter}"],
        Transplant_Sex=row[f"uktr_dsex{transplant_counter}"],
        Cause_Of_Failure=row[f"uktr_cof{transplant_counter}"],
        Cause_Of_Failure_Text=row[f"uktr_other_cof_text{transplant_counter}"],
        CIT_Mins=row[f"uktr_cit_mins{transplant_counter}"],
        HLA_Mismatch=row[f"uktr_hla_mm{transplant_counter}"],
        UKT_SUSPENSION=row[f"uktr_suspension_{transplant_counter}"],
    )


def create_logs():
    logging.config.dictConfig(yaml.safe_load(open("logconf.yaml")))
    return logging.getLogger("nhsbt_import")


def create_session():
    driver = "SQL+Server+Native+Client+11.0"
    engine = create_engine(f"mssql+pyodbc://rr-sql-live/renalreg?driver={driver}")
    return Session(engine, future=True)


def get_error_file_path(input_file):
    folder, fn = os.path.split(input_file)
    return os.path.join(folder, "NHSBT_Errors.xls")


def update_nhsbt_patient(incoming_patient, existing_patient):
    # Incoming RR will always be None so preserve existing
    existing_rr = existing_patient.rr_no
    existing_patient = incoming_patient
    existing_patient.rr_no = existing_rr
    return existing_patient


def update_nhsbt_transplant(incoming_transplant, existing_transplant):
    # Incoming RR will always be None so preserve existing
    existing_rr = existing_transplant.rr_no
    existing_transplant = incoming_transplant
    existing_transplant.rr_no = existing_rr
    return existing_transplant


# TODO: Rename function
def make_patient_match_row(incoming_patient, existing_patient, match_type):
    # TODO: Add the other columns CHI etc
    patient_row = {
        "Match Type": match_type,
        "UKTSSA_No": incoming_patient.uktssa_no,
        "File RR_No": None,
        "File Surname": incoming_patient.surname,
        "File Forename": incoming_patient.forename,
        "File Sex": incoming_patient.sex,
        "File Date Birth": incoming_patient.ukt_date_birth,
        "File NHS Number": incoming_patient.new_nhs_no,
    }

    if existing_patient:
        patient_row["DB RR_No"] = existing_patient.rr_no
        patient_row["DB Surname"] = existing_patient.surname
        patient_row["DB Forename"] = existing_patient.forename
        patient_row["DB Sex"] = existing_patient.sex
        patient_row["DB Date Birth"] = existing_patient.ukt_date_birth
        patient_row["DB NHS Number"] = existing_patient.new_nhs_no

    return patient_row


def make_transplant_match_row(incoming_transplant, existing_transplant, match_type):
    transplant_row = {
        "Match Type": match_type,
        "UKTSSA_No": incoming_transplant.uktssa_no,
        "Transplant ID - File": incoming_transplant.transplant_id,
        "Registration ID - File": incoming_transplant.registration_id,
        "Transplant Date - File": incoming_transplant.transplant_date,
        "Transplant Type - File": incoming_transplant.transplant_type,
        "Transplant Organ - File": incoming_transplant.transplant_organ,
        "Transplant Unit - File": incoming_transplant.transplant_unit,
        "Registration Date - File": incoming_transplant.registration_date,
        "Registration Date Type - File": incoming_transplant.registration_date_type,
        "Registration End Date - File": incoming_transplant.registration_end_date,
        "Registration End Status - File": incoming_transplant.registration_end_status,
        "Transplant Consideration - File": incoming_transplant.transplant_consideration,
        "Transplant Dialysis - File": incoming_transplant.transplant_dialysis,
        "Transplant Relationship - File": incoming_transplant.transplant_relationship,
        "Transplant Sex - File": incoming_transplant.transplant_sex,
        "Cause of Failure - File": incoming_transplant.cause_of_failure,
        "Cause of Failure Text - File": incoming_transplant.cause_of_failure_text,
        "CIT Mins - File": incoming_transplant.cit_mins,
        "HLA Mismatch - File": incoming_transplant.hla_mismatch,
        "UKT Suspension - File": incoming_transplant.ukt_suspension,
    }

    if existing_transplant:
        transplant_row["Transplant ID - DB"] = existing_transplant.transplant_id
        transplant_row["Registration ID - DB"] = existing_transplant.registration_id
        transplant_row["Transplant Date - DB"] = existing_transplant.transplant_date
        transplant_row["Transplant Type - DB"] = existing_transplant.transplant_type
        transplant_row["Transplant Organ - DB"] = existing_transplant.transplant_organ
        transplant_row["Transplant Unit - DB"] = existing_transplant.transplant_unit
        transplant_row["Registration Date - DB"] = existing_transplant.registration_date
        transplant_row[
            "Registration Date Type - DB"
        ] = existing_transplant.registration_date_type
        transplant_row[
            "Registration End Date - DB"
        ] = existing_transplant.registration_end_date
        transplant_row[
            "Registration End Status - DB"
        ] = existing_transplant.registration_end_status
        transplant_row[
            "Transplant Consideration - DB"
        ] = existing_transplant.transplant_consideration
        transplant_row[
            "Transplant Dialysis - DB"
        ] = existing_transplant.transplant_dialysis
        transplant_row[
            "Transplant Relationship - DB"
        ] = existing_transplant.transplant_relationship
        transplant_row["Transplant Sex - DB"] = existing_transplant.transplant_sex
        transplant_row["Cause of Failure - DB"] = existing_transplant.cause_of_failure
        transplant_row[
            "Cause of Failure Text - DB"
        ] = existing_transplant.cause_of_failure_text
        transplant_row["CIT Mins - DB"] = existing_transplant.cit_mins
        transplant_row["HLA Mismatch - DB"] = existing_transplant.hla_mismatch
        transplant_row["UKT Suspension - DB"] = existing_transplant.ukt_suspension

    return transplant_row
