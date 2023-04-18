import argparse
import logging
import logging.config
import os
import sys
from typing import Any

import pandas as pd
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from ukrr_models.nhsbt_models import UKT_Patient, UKT_Transplant


def add_df_row(df, row):
    row = pd.DataFrame(row, index=[0])
    return pd.concat([df, row], ignore_index=True)


def args_parse(argv=None):
    parser = argparse.ArgumentParser(description="nhsbt_import")
    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        help="Specify the directory that holds the input file",
    )

    args = parser.parse_args(argv)

    if len(sys.argv) <= 1:
        print(parser.format_help())
        sys.exit(1)

    if not os.path.exists(args.directory):
        raise NotADirectoryError(f"{args.directory} not found")

    if not os.path.isdir(args.directory):
        raise NotADirectoryError(f"Path is not a directory: {args.directory}")

    return args


def create_df(name, columns):
    return pd.DataFrame(columns=columns[name])


def create_incoming_patient(index, row, log):
    uktssa_no = row["UKTR_ID"]
    if pd.isna(uktssa_no) or uktssa_no == 0:
        message = f"UKTR_ID must be a valid number, check row {index}"
        log.warning(message)
        raise ValueError(message)

    return UKT_Patient(
        uktssa_no=int(uktssa_no),
        surname=row["UKTR_RSURNAME"],
        forename=row["UKTR_RFORENAME"],
        sex=row["UKTR_RSEX"],
        post_code=row["UKTR_RPOSTCODE"],
        new_nhs_no=row["UKTR_RNHS_NO"],
        chi_no=row["UKTR_RCHI_NO_SCOT"],
        hsc_no=row["UKTR_RCHI_NO_NI"],
        rr_no=None,
        ukt_date_death=format_date(row["UKTR_DDATE"]),
        ukt_date_birth=format_date(row["UKTR_RDOB"]),
    )


def create_incoming_transplant(row, transplant_counter):
    return UKT_Transplant(
        transplant_id=row[f"uktr_tx_id{transplant_counter}"],
        uktssa_no=row["UKTR_ID"],
        transplant_date=row[f"uktr_txdate{transplant_counter}"],
        transplant_type=row[f"uktr_dgrp{transplant_counter}"],
        transplant_organ=row[f"uktr_tx_type{transplant_counter}"],
        transplant_unit=row[f"uktr_tx_unit{transplant_counter}"],
        ukt_fail_date=row[f"uktr_faildate{transplant_counter}"],
        rr_no=None,
        registration_id=f'{int(row["UKTR_ID"])}_{transplant_counter}',
        registration_date=row[f"uktr_date_on{transplant_counter}"],
        registration_date_type=row[f"uktr_list_status{transplant_counter}"],
        registration_end_date=row[f"uktr_removal_date{transplant_counter}"],
        registration_end_status=row[f"uktr_endstat{transplant_counter}"],
        transplant_consideration=row[f"uktr_tx_list{transplant_counter}"],
        transplant_dialysis=row[f"uktr_dial_at_tx{transplant_counter}"],
        transplant_relationship=row[f"uktr_relationship{transplant_counter}"],
        transplant_sex=row[f"uktr_dsex{transplant_counter}"],
        cause_of_failure=row[f"uktr_cof{transplant_counter}"],
        cause_of_failure_text=row[f"uktr_other_cof_text{transplant_counter}"],
        cit_mins=row[f"uktr_cit_mins{transplant_counter}"],
        hla_mismatch=row[f"uktr_hla_mm{transplant_counter}"],
        ukt_suspension=row[f"uktr_suspension_{transplant_counter}"],
    )


def create_logs(directory):
    errors_file_path = os.path.abspath(f"{directory}/errors.log").replace("\\", "/")[2:]

    logging.config.fileConfig(
        fname="logconf.conf",
        disable_existing_loggers=False,
        defaults={"logfilename": errors_file_path},
    )
    return logging.getLogger("nhsbt_import")


def create_session():
    driver = "SQL+Server+Native+Client+11.0"
    engine = create_engine(f"mssql+pyodbc://rr-sql-live/renalreg?driver={driver}")
    return Session(engine, future=True)


def format_date(str_date: Any):
    if isinstance(str_date, str):
        return parse(str_date)
    return


def get_input_file_path(directory, log):
    csv_list = [file for file in os.listdir(directory) if file.endswith(".csv")]

    if len(csv_list) > 1:
        log.warning()
        raise ValueError(
            f"Expected to find one import CSV file in {directory}, but found {len(csv_list)} files."
        )

    return os.path.join(directory, csv_list[0])


def make_patient_match_row(match_type, incoming_patient, existing_patient):
    # TODO: Add the other columns CHI etc
    patient_row = {
        "Match Type": match_type,
        "UKTSSA_No": incoming_patient.uktssa_no,
        "Surname - NHSBT": incoming_patient.surname,
        "Forename - NHSBT": incoming_patient.forename,
        "Sex - NHSBT": incoming_patient.sex,
        "Date Birth - NHSBT": incoming_patient.ukt_date_birth,
        "NHS Number - NHSBT": incoming_patient.new_nhs_no,
    }

    if existing_patient:
        patient_row["RR_No"] = existing_patient.rr_no
        patient_row["Surname - RR"] = existing_patient.surname
        patient_row["Forename - RR"] = existing_patient.forename
        patient_row["Sex - RR"] = existing_patient.sex
        patient_row["Date Birth - RR"] = existing_patient.ukt_date_birth
        patient_row["NHS Number - RR"] = existing_patient.new_nhs_no

    return patient_row


def make_transplant_match_row(match_type, incoming_transplant, existing_transplant):
    transplant_row = {
        "Match Type": match_type,
        "UKTSSA_No": incoming_transplant.uktssa_no,
        "Transplant ID - NHSBT": incoming_transplant.transplant_id,
        "Registration ID - NHSBT": incoming_transplant.registration_id,
        "Transplant Date - NHSBT": incoming_transplant.transplant_date,
        "Transplant Type - NHSBT": incoming_transplant.transplant_type,
        "Transplant Organ - NHSBT": incoming_transplant.transplant_organ,
        "Transplant Unit - NHSBT": incoming_transplant.transplant_unit,
        "Registration Date - NHSBT": incoming_transplant.registration_date,
        "Registration Date Type - NHSBT": incoming_transplant.registration_date_type,
        "Registration End Date - NHSBT": incoming_transplant.registration_end_date,
        "Registration End Status - NHSBT": incoming_transplant.registration_end_status,
        "Transplant Consideration - NHSBT": incoming_transplant.transplant_consideration,
        "Transplant Dialysis - NHSBT": incoming_transplant.transplant_dialysis,
        "Transplant Relationship - NHSBT": incoming_transplant.transplant_relationship,
        "Transplant Sex - NHSBT": incoming_transplant.transplant_sex,
        "Cause of Failure - NHSBT": incoming_transplant.cause_of_failure,
        "Cause of Failure Text - NHSBT": incoming_transplant.cause_of_failure_text,
        "CIT Mins - NHSBT": incoming_transplant.cit_mins,
        "HLA Mismatch - NHSBT": incoming_transplant.hla_mismatch,
        "UKT Suspension - NHSBT": incoming_transplant.ukt_suspension,
    }

    if existing_transplant:
        transplant_row["Transplant ID - RR"] = existing_transplant.transplant_id
        transplant_row["Registration ID - RR"] = existing_transplant.registration_id
        transplant_row["Transplant Date - RR"] = existing_transplant.transplant_date
        transplant_row["Transplant Type - RR"] = existing_transplant.transplant_type
        transplant_row["Transplant Organ - RR"] = existing_transplant.transplant_organ
        transplant_row["Transplant Unit - RR"] = existing_transplant.transplant_unit
        transplant_row["Registration Date - RR"] = existing_transplant.registration_date
        transplant_row[
            "Registration Date Type - RR"
        ] = existing_transplant.registration_date_type
        transplant_row[
            "Registration End Date - RR"
        ] = existing_transplant.registration_end_date
        transplant_row[
            "Registration End Status - RR"
        ] = existing_transplant.registration_end_status
        transplant_row[
            "Transplant Consideration - RR"
        ] = existing_transplant.transplant_consideration
        transplant_row[
            "Transplant Dialysis - RR"
        ] = existing_transplant.transplant_dialysis
        transplant_row[
            "Transplant Relationship - RR"
        ] = existing_transplant.transplant_relationship
        transplant_row["Transplant Sex - RR"] = existing_transplant.transplant_sex
        transplant_row["Cause of Failure - RR"] = existing_transplant.cause_of_failure
        transplant_row[
            "Cause of Failure Text - RR"
        ] = existing_transplant.cause_of_failure_text
        transplant_row["CIT Mins - RR"] = existing_transplant.cit_mins
        transplant_row["HLA Mismatch - RR"] = existing_transplant.hla_mismatch
        transplant_row["UKT Suspension - RR"] = existing_transplant.ukt_suspension

    return transplant_row


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
