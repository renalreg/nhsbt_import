import os
import sys

import pandas as pd
from ukrr_models.nhsbt_models import UKT_Patient, UKT_Transplant

from nhsbt_import.df_columns import df_columns
from nhsbt_import.utils import (
    args_parse,
    create_df,
    create_incoming_patient,
    create_logs,
    create_session,
    get_error_file_path,
    make_match_row,
    update_nhsbt_patient,
)

# from ukrr_models.rr_models import UKRR_Deleted_Patient, UKRR_Patient


# from datetime import datetime
# from rr_reports import ExcelLib


# def format_date(str_date: str):
#     date_formats = ["%d%b%Y", "%d-%b-%y"]
#     formatted_date = None
#     for date_format in date_formats:
#         try:
#             formatted_date = datetime.strptime(str_date, date_format).date()
#         except Exception:
#             pass
#     if formatted_date is None:
#         print(str_date)
#         raise Exception
#     return formatted_date


# def run(csv_reader, error_file, log):

#         for i, x in enumerate((3, 22, 41, 60, 79, 98)):
#             registration_id = str(uktssa_no) + "_" + str(i + 1)

#             registration_date = row[x]  # 1
#             x += 1
#             if registration_date in ("", None):
#                 log.debug("No registration date for {}".format(registration_id))
#                 continue
#             registration_date = format_date(registration_date)

#             registration_date_type = row[x]  # 2
#             x += 1
#             if registration_date_type in ("", None):
#                 registration_date_type = ""

#             registration_end_status = row[x]  # 3
#             x += 1
#             if registration_end_status in ("", None):
#                 registration_end_status = ""

#             transplant_consideration = row[x]  # 4
#             x += 1
#             if transplant_consideration in ("", None):
#                 transplant_consideration = ""

#             ukt_suspension = row[x]  # 5
#             x += 1
#             if ukt_suspension in ("", None):
#                 ukt_suspension = ""

#             registration_end_date = row[x]  # 6
#             x += 1
#             if registration_end_date in ("", None):
#                 registration_end_date = None
#             else:
#                 registration_end_date = format_date(registration_end_date)

#             transplant_id = row[x]  # 7
#             x += 1
#             if transplant_id in ("", None):
#                 transplant_id = None
#             else:
#                 transplant_id = int(transplant_id)

#             transplant_list.append(registration_id)

#             transplant_date = row[x]  # 8
#             x += 1
#             if transplant_date in ("", None):
#                 transplant_date = None
#             else:
#                 transplant_date = format_date(transplant_date)

#             transplant_type = row[x]  # 9
#             x += 1
#             if transplant_type in ("", None):
#                 transplant_type = ""

#             transplant_sex = row[x]  # 10
#             x += 1
#             if transplant_sex in ("", None):
#                 transplant_sex = ""

#             transplant_relationship = row[x]  # 11
#             x += 1
#             if transplant_relationship in ("", None):
#                 transplant_relationship = ""

#             transplant_organ = row[x]  # 12
#             x += 1
#             if transplant_organ in ("", None):
#                 transplant_organ = ""

#             transplant_unit = row[x]  # 13
#             x += 1
#             if transplant_unit in ("", None):
#                 transplant_unit = ""

#             ukt_fail_date = row[x]  # 14
#             x += 1
#             if ukt_fail_date in ("", None):
#                 ukt_fail_date = None
#             else:
#                 ukt_fail_date = format_date(ukt_fail_date)

#             transplant_dialysis = row[x]  # 15
#             x += 1
#             if transplant_dialysis in ("", None):
#                 transplant_dialysis = ""

#             cit_mins = row[x]  # 16
#             x += 1
#             if cit_mins in ("", None):
#                 cit_mins = ""

#             hla_mismatch = row[x]  # 17
#             x += 1
#             if hla_mismatch in ("", None):
#                 hla_mismatch = ""

#             cause_of_failure = row[x]  # 18
#             x += 1
#             if cause_of_failure in ("", None):
#                 cause_of_failure = ""

#             cause_of_failure_text = row[x]  # 19
#             x += 1
#             if cause_of_failure_text in ("", None):
#                 cause_of_failure_text = ""

#             results = (
#                 Session.query(UKT_Transplant)
#                 .filter_by(registration_id=registration_id)
#                 .all()
#             )

#             # Record exists - update it
#             if len(results) > 0:
#                 ukt_transplant = results[0]
#                 log.info("Updating record")
#                 # No need to update Registration ID as it was used
#                 # for matching. Or UKTSSA_No as they're related.

#                 if rr_no != ukt_transplant.rr_no:
#                     ukt_transplant.rr_no = rr_no

#                 if registration_date != ukt_transplant.registration_date:
#                     ukt_transplant.registration_date = registration_date

#                 if registration_date_type != ukt_transplant.registration_date_type:
#                     ukt_transplant.registration_date_type = registration_date_type

#                 if registration_end_status != ukt_transplant.registration_end_status:
#                     ukt_transplant.registration_end_status = registration_end_status

#                 if transplant_consideration != ukt_transplant.transplant_consideration:
#                     ukt_transplant.transplant_consideration = transplant_consideration

#                 if ukt_suspension != ukt_transplant.ukt_suspension:
#                     ukt_transplant.ukt_suspension = ukt_suspension

#                 if registration_end_date != ukt_transplant.registration_end_date:
#                     if ukt_transplant.registration_end_date is not None:
#                         excel_error_wb.Sheets["Transplant Field Differences"].WriteRow(
#                             (
#                                 uktssa_no,
#                                 registration_id,
#                                 "Registration End Date",
#                                 registration_end_date,
#                                 ukt_transplant.registration_end_date,
#                             )
#                         )
#                     ukt_transplant.registration_end_date = registration_end_date

#                 if transplant_id != ukt_transplant.transplant_id:
#                     ukt_transplant.transplant_id = transplant_id

#                 if transplant_date != ukt_transplant.transplant_date:
#                     ukt_transplant.transplant_date = transplant_date

#                 if transplant_type != ukt_transplant.transplant_type:
#                     ukt_transplant.transplant_type = transplant_type

#                 if transplant_sex != ukt_transplant.transplant_sex:
#                     ukt_transplant.transplant_sex = transplant_sex

#                 if transplant_relationship != ukt_transplant.transplant_relationship:
#                     ukt_transplant.transplant_relationship = transplant_relationship

#                 if transplant_organ != ukt_transplant.transplant_organ:
#                     ukt_transplant.transplant_organ = transplant_organ

#                 # TODO: This might benefit from all being converted to ASCII
#                 if transplant_unit != ukt_transplant.transplant_unit:
#                     ukt_transplant.transplant_unit = transplant_unit

#                 if ukt_fail_date != ukt_transplant.ukt_fail_date:
#                     ukt_transplant.ukt_fail_date = ukt_fail_date

#                 if transplant_dialysis != ukt_transplant.transplant_dialysis:
#                     ukt_transplant.transplant_dialysis = transplant_dialysis

#                 if cit_mins != ukt_transplant.cit_mins:
#                     ukt_transplant.cit_mins = cit_mins

#                 if hla_mismatch != ukt_transplant.hla_mismatch:
#                     ukt_transplant.hla_mismatch = hla_mismatch

#                 if cause_of_failure != ukt_transplant.cause_of_failure:
#                     ukt_transplant.cause_of_failure = cause_of_failure

#                 if cause_of_failure_text != ukt_transplant.cause_of_failure_text:
#                     ukt_transplant.cause_of_failure_text = cause_of_failure_text

#             # Mew Record
#             else:
#                 log.info("Add record to database")
#                 ukt_transplant = UKT_Transplant(
#                     uktssa_no=uktssa_no,
#                     rr_no=rr_no,
#                     registration_id=registration_id,
#                     registration_date=registration_date,
#                     registration_date_type=registration_date_type,
#                     registration_end_status=registration_end_status,
#                     transplant_consideration=transplant_consideration,
#                     ukt_suspension=ukt_suspension,
#                     registration_end_date=registration_end_date,
#                     transplant_id=transplant_id,
#                     transplant_date=transplant_date,
#                     transplant_type=transplant_type,
#                     transplant_sex=transplant_sex,
#                     transplant_relationship=transplant_relationship,
#                     transplant_organ=transplant_organ,
#                     transplant_unit=transplant_unit,
#                     ukt_fail_date=ukt_fail_date,
#                     transplant_dialysis=transplant_dialysis,
#                     cit_mins=cit_mins,
#                     hla_mismatch=hla_mismatch,
#                     cause_of_failure=cause_of_failure,
#                     cause_of_failure_text=cause_of_failure_text,
#                 )
#                 Session.add(ukt_transplant)

#                 session.commit()

#         Cursor = Engine.connect()

#     sql_string = """
#     SELECT
#         DISTINCT UKTSSA_NO, RR_NO
#     FROM
#         UKT_PATIENTS
#     WHERE
#         RR_NO IS NOT NULL"""

#     results = Cursor.execute(sql_string).fetchall()

#     missing_patient_count = 0
#     for row in results:
#         if not (row[0] in patient_list):
#             missing_patient_count = missing_patient_count + 1
#             excel_error_wb.Sheets["Missing Patients"].WriteRow((row[0], row[1]))

#     log.warning("Missing Prior UKT Patients {}".format(missing_patient_count))

#     sql_string = """
#     SELECT
#         DISTINCT REGISTRATION_ID
#     FROM
#         UKT_TRANSPLANTS
#     WHERE
#         TRANSPLANT_ID IS NOT NULL AND
#         RR_NO IS NOT NULL AND
#         RR_NO < 999900000
#     """

#     results = Cursor.execute(sql_string).fetchall()


#     transplant_list = set(transplant_list)
#     # TODO: For Subsequent updates it may make sense to look for missing registrations
#     for row in results:
#         if row[0] not in transplant_list:
#             log.warning("Missing Transplant {}".format(row[0]))
#             excel_error_wb.Sheets["Missing Transplants"].WriteRow((row[0],))
#     log.info("Complete error spreadsheet {}".format(error_file))
#     excel_error_wb.Save(error_file)


def import_patient(row, session, log, match_type_df):
    if (
        incoming_patient := create_incoming_patient(row)
    ).uktssa_no.notna() and incoming_patient.uktssa_no != 0:
        results = (
            session.query(UKT_Patient)
            .filter_by(uktssa_no=incoming_patient.uktssa_no)
            .all()
        )
        match_type = None

        if len(results) == 1:
            log.info(f"UKT Patient {incoming_patient.uktssa_no} found in database")
            log.info("Updating record")
            match_type = "Update"
            existing_patient = results[0]
            match_row = make_match_row(match_type, incoming_patient, existing_patient)
            match_type_df.append(match_row, ignore_index=True)
            existing_patient = update_nhsbt_patient(incoming_patient, existing_patient)
            session.commit()

        if len(results) == 0:
            log.info("Add patient")
            match_type = "New"
            match_row = make_match_row(
                match_type, incoming_patient, existing_patient=None
            )
            match_type_df.append(match_row, ignore_index=True)
            session.add(incoming_patient)
        else:
            log.error(f"{incoming_patient.uktssa_no} in the database multiple times")

        return match_type


def nhsbt_import(input_file, log, session):
    # TODO: Some of these dataframes are redundant
    nhsbt_df = pd.read_csv(input_file)
    match_type_df = create_df("match_type_df", df_columns)
    patient_field_differences_df = create_df("patient_field_differences_df", df_columns)
    transplant_field_differences_df = create_df(
        "transplant_field_differences_df", df_columns
    )
    invalid_postcode_df = create_df("invalid_postcode_df", df_columns)
    invalid_nhs_number_df = create_df("invalid_nhs_number_df", df_columns)
    missing_patient_df = create_df("missing_patient_df", df_columns)
    missing_transplant_df = create_df("missing_transplant_df", df_columns)

    patient_list = []
    transplant_list = []

    for index, row in nhsbt_df.iterrows():
        log.info(f"on line {index}")
        if match_type := import_patient(row, session, log, match_type_df):
            patient_list.append(row["UKTR_ID"])

    # TODO: Is it still worth checking the deleted table?
    # match_type = "Match to Deleted Patient"


def check_input_file():
    args, args_help = args_parse()
    input_file = args.input_file

    if len(sys.argv) <= 1:
        print(args_help)
        sys.exit(1)

    if not os.path.exists(input_file):
        print(
            f"""
        Input File doesn't exist. Check file path
        {input_file}"""
        )
        sys.exit(1)

    return input_file


def main():
    log = create_logs()
    session = create_session()
    input_file_path = check_input_file()
    error_file_path = get_error_file_path(input_file_path)
    nhsbt_import(input_file_path, log, session=None)
    session.close()


if __name__ == "__main__":
    main()
