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
    make_patient_match_row,
    make_transplant_match_row,
    update_nhsbt_patient,
    create_incoming_transplant,
    update_nhsbt_transplant,
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


def run(csv_reader, error_file, log):
    pass


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


def import_patient(row, session, log, output_dfs):
    match_type = None
    # If a ukt number exists in the row of the file search the DB
    if (
        incoming_patient := create_incoming_patient(row)
    ).uktssa_no.notna() and incoming_patient.uktssa_no != 0:
        # If patient exists in DB update if required
        if (
            results := (
                session.query(UKT_Patient)
                .filter_by(uktssa_no=incoming_patient.uktssa_no)
                .all()
            )
            == 1
        ):
            log.info(f"UKT Patient {incoming_patient.uktssa_no} found in database")
            existing_patient = results[0]

            if existing_patient != incoming_patient:
                log.info("Updating record")
                match_type = "Update"

                match_row = make_patient_match_row(
                    match_type, incoming_patient, existing_patient
                )

                output_dfs["updated_patients_df"].append(match_row, ignore_index=True)

                existing_patient = update_nhsbt_patient(
                    incoming_patient, existing_patient
                )

                session.commit()
            else:
                log.info("No Update required")

        # If patient doesn't exist in DB, add
        elif len(results) == 0:
            log.info("Add patient")
            match_type = "New"

            match_row = make_patient_match_row(
                match_type, incoming_patient, existing_patient=None
            )

            output_dfs["new_patients_df"].append(match_row, ignore_index=True)

            session.add(incoming_patient)
        else:
            log.error(f"{incoming_patient.uktssa_no} in the database multiple times")

        return match_type


def import_transplants(row, session, log, output_dfs):
    # Max transplants is determined by what is sent in the file
    # Adjust if more columns of transplants are sent
    max_transplants = 6
    transplant_counter = 1
    transplant_match_types = {}

    while transplant_counter <= max_transplants:
        # Minimum for a transplant is a registered date
        if row[f"uktr_date_on{transplant_counter}"]:
            incoming_transplant = create_incoming_transplant(row, transplant_counter)

            if (
                results := session.query(UKT_Transplant)
                .filter_by(registration_id=incoming_transplant.registration_id)
                .all()
                == 1
            ):
                log.info(
                    f"Transplant ID {incoming_transplant.registration_id} found in database"
                )

                existing_transplant = results[0]

                if existing_transplant != incoming_transplant:
                    log.info("Updating record")

                    match_type = "Update"
                    transplant_match_types[
                        incoming_transplant.registration_id
                    ] = match_type

                    match_row = make_transplant_match_row(
                        match_type, incoming_transplant, existing_transplant
                    )

                    output_dfs["updated_transplants_df"].append(
                        match_row, ignore_index=True
                    )

                    existing_transplant = update_nhsbt_transplant(
                        incoming_transplant, existing_transplant
                    )

                    session.commit()
                else:
                    log.info("No Update required")

            elif len(results) == 0:
                log.info("Add transplant")

                match_type = "New"
                transplant_match_types[incoming_transplant.registration_id] = match_type

                match_row = make_transplant_match_row(
                    match_type, incoming_transplant, existing_transplant=None
                )

                output_dfs["new_transplant_df"].append(match_row, ignore_index=True)

                session.add(incoming_transplant)
            else:
                log.error(
                    f"{incoming_transplant.registration_id} in the database multiple times"
                )


def nhsbt_import(input_file, log, session):
    nhsbt_df = pd.read_csv(input_file)
    output_dfs = {
        "new_patients_df": create_df("new_patients_df", df_columns),
        "updated_patients_df": create_df("updated_patients_df", df_columns),
        "new_transplant_df": create_df("new_transplant_df", df_columns),
        "updated_transplants_df": create_df("updated_transplants_df", df_columns),
    }

    patient_list = []
    transplant_list = []

    for index, row in nhsbt_df.iterrows():
        log.info(f"on line {index}")
        if match_type := import_patient(row, session, log, output_dfs):
            import_transplants(row, session, log, output_dfs)

    writer = pd.ExcelWriter("import_logs.xlsx", engine="xlsxwriter")
    for sheet_name, df in output_dfs.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)

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
