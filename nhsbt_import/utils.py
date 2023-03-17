import argparse
import logging
import logging.config
import os

import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from ukrr_models.nhsbt_models import UKT_Patient


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


def make_match_row(incoming_patient, existing_patient, match_type):
    # TODO: Add the other columns
    if existing_patient:
        return {
            "Match Type": match_type,
            "UKTSSA_No": incoming_patient.uktssa_no,
            "File RR_No": None,
            "File Surname": incoming_patient.surname,
            "File Forename": incoming_patient.forename,
            "File Sex": incoming_patient.sex,
            "File Date Birth": incoming_patient.ukt_date_birth,
            "File NHS Number": incoming_patient.new_nhs_no,
            "DB RR_No": existing_patient.rr_no,
            "DB Surname": existing_patient.surname,
            "DB Forename": existing_patient.forename,
            "DB Sex": existing_patient.sex,
            "DB Date Birth": existing_patient.ukt_date_birth,
            "DB NHS Number": existing_patient.new_nhs_no,
        }
    else:
        return {
            "Match Type": match_type,
            "UKTSSA_No": incoming_patient.uktssa_no,
            "File RR_No": None,
            "File Surname": incoming_patient.surname,
            "File Forename": incoming_patient.forename,
            "File Sex": incoming_patient.sex,
            "File Date Birth": incoming_patient.ukt_date_birth,
            "File NHS Number": incoming_patient.new_nhs_no,
            "DB RR_No": None,
            "DB Surname": None,
            "DB Forename": None,
            "DB Sex": None,
            "DB Date Birth": None,
            "DB NHS Number": None,
        }
