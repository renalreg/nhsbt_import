import os

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from ukrr_models.nhsbt_models import UKT_Patient, UKT_Transplant

from nhsbt_import.df_columns import df_columns
from nhsbt_import.utils import (
    add_df_row,
    args_parse,
    create_df,
    create_incoming_patient,
    create_incoming_transplant,
    create_logs,
    create_session,
    get_input_file_path,
    make_patient_match_row,
    make_transplant_match_row,
    update_nhsbt_patient,
    update_nhsbt_transplant,
)


def run(csv_reader, error_file):
    """
    George

    There is a whole load of code here that I don't think is require any longer.
    First there is a search for missing patients and then a search for missing transplants
    which I'm hoping will no longer be an issue as we are just getting everything.

    The other thing it does that I'm not sure is worth it any more is to check for matches
    against the deleted table. This might be something that is worth doing but I'm not
    sure this is the right place as this is really only concerned with loading them.
    Not 100% sure about that last part though
    """
    pass

    # TODO: Is it still worth checking the deleted table?
    # match_type = "Match to Deleted Patient"


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


def import_patient(index, row, session, output_dfs, log):
    match_type = None
    # If a ukt number exists in the row of the file search the DB
    incoming_patient = create_incoming_patient(index, row, log)
    # If patient exists in DB update if required
    if (
        len(
            results := (
                session.query(UKT_Patient)
                .filter_by(uktssa_no=incoming_patient.uktssa_no)
                .all()
            )
        )
        == 1
    ):
        log.info(f"UKT Patient {incoming_patient.uktssa_no} found in database")
        existing_patient = results[0]

        if existing_patient != incoming_patient:
            log.info("Updating patient")
            # TODO: Starting to think match type is pointless
            match_type = "Update"

            match_row = make_patient_match_row(
                match_type, incoming_patient, existing_patient
            )

            output_dfs["updated_patients"] = add_df_row(
                output_dfs["updated_patients"], match_row
            )

            existing_patient = update_nhsbt_patient(incoming_patient, existing_patient)

            # session.commit()
        else:
            log.info("No Update required")

    # If patient doesn't exist in DB, add
    elif len(results) == 0:
        log.info(f"Adding patient {incoming_patient.uktssa_no}")
        match_type = "New"

        match_row = make_patient_match_row(
            match_type, incoming_patient, existing_patient=None
        )

        output_dfs["new_patients"] = add_df_row(output_dfs["new_patients"], match_row)

        # session.add(incoming_patient)
        # session.commit()
    else:
        log.error(f"{incoming_patient.uktssa_no} in the database multiple times")

    return match_type


def import_transplants(row, session, output_dfs, log):
    # Max transplants is determined by what is sent in the file
    # Adjust if more columns of transplants are sent
    # TODO: Might be better of in an env file
    max_transplants = 6
    transplant_counter = 1
    transplant_match_types = {}

    while transplant_counter <= max_transplants and not pd.isna(
        row[f"uktr_date_on{transplant_counter}"]
    ):
        incoming_transplant = create_incoming_transplant(row, transplant_counter)

        if (
            len(
                results := session.query(UKT_Transplant)
                .filter_by(registration_id=incoming_transplant.registration_id)
                .all()
            )
            == 1
        ):
            log.info(
                f"Registration ID {incoming_transplant.registration_id} found in database"
            )

            existing_transplant = results[0]

            if existing_transplant == incoming_transplant:
                log.info("No Update required")

            else:
                log.info("Updating transplant")

                match_type = "Update"
                transplant_match_types[incoming_transplant.registration_id] = match_type

                match_row = make_transplant_match_row(
                    match_type, incoming_transplant, existing_transplant
                )

                output_dfs["updated_transplants"] = add_df_row(
                    output_dfs["updated_transplants"], match_row
                )

                existing_transplant = update_nhsbt_transplant(
                    incoming_transplant, existing_transplant
                )

                # session.commit()
        elif len(results) == 0:
            log.info(f"Adding transplant {incoming_transplant.registration_id}")

            match_type = "New"
            transplant_match_types[incoming_transplant.registration_id] = match_type

            match_row = make_transplant_match_row(
                match_type, incoming_transplant, existing_transplant=None
            )

            output_dfs["new_transplant"] = add_df_row(
                output_dfs["new_transplant"], match_row
            )

            # session.add(incoming_transplant)
            # session.commit()
        else:
            log.error(
                f"{incoming_transplant.registration_id} in the database multiple times"
            )
        transplant_counter += 1


def nhsbt_import(input_file_path, audit_file_path, session, log):
    # TODO: The first step is cleaning this file with Notepad++ but I'm sure we could do this in code
    nhsbt_df = pd.read_csv(input_file_path)
    output_dfs = {
        "new_patients": create_df("new_patients", df_columns),
        "updated_patients": create_df("updated_patients", df_columns),
        "new_transplant": create_df("new_transplant", df_columns),
        "updated_transplants": create_df("updated_transplant", df_columns),
    }

    output_dfs["new_transplant"]["UKT Suspension - NHSBT"] = output_dfs[
        "new_transplant"
    ]["UKT Suspension - NHSBT"].astype(bool)

    output_dfs["updated_transplants"]["UKT Suspension - NHSBT"] = output_dfs[
        "updated_transplants"
    ]["UKT Suspension - NHSBT"].astype(bool)

    output_dfs["updated_transplants"]["UKT Suspension - RR"] = output_dfs[
        "updated_transplants"
    ]["UKT Suspension - RR"].astype(bool)

    patient_list = []
    transplant_list = []

    for index, row in nhsbt_df.iterrows():
        index += 1
        log.info(f"on line {index + 1}")
        if match_type := import_patient(index, row, session, output_dfs, log):
            import_transplants(row, session, output_dfs, log)

    wb = Workbook()
    wb.remove(wb["Sheet"])

    for sheet_name, df in output_dfs.items():
        ws = wb.create_sheet(sheet_name)
        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)

        for i, col in enumerate(df.columns):
            column = get_column_letter(i + 1)
            header_length = len(col)
            cell_length = max(df[col].astype(str).map(len).max(), header_length)
            adjusted_width = cell_length + 2
            ws.column_dimensions[column].width = adjusted_width

        for row in ws:
            for cell in row:
                cell.alignment = Alignment(horizontal="center")

    wb.save(audit_file_path)


def main():
    args = args_parse()
    log = create_logs(args.directory)
    input_file_path = get_input_file_path(args.directory, log)
    audit_file_path = os.path.join(args.directory, "audit.xlsx")
    session = create_session()
    nhsbt_import(input_file_path, audit_file_path, session, log)
    session.close()


if __name__ == "__main__":
    main()
