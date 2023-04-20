import os

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows
from ukrr_models.nhsbt_models import UKT_Patient, UKT_Transplant
from ukrr_models.rr_models import UKRR_Deleted_Patient

from nhsbt_import.df_columns import df_columns
from nhsbt_import.utils import (
    add_df_row,
    args_parse,
    check_missing_patients,
    check_missing_transplants,
    create_output_dfs,
    create_incoming_patient,
    create_incoming_transplant,
    create_logs,
    create_session,
    deleted_patient_check,
    get_input_file_path,
    make_patient_match_row,
    make_transplant_match_row,
    update_nhsbt_patient,
    update_nhsbt_transplant,
)


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
    transplant_ids = []

    while transplant_counter <= max_transplants and not pd.isna(
        row[f"uktr_date_on{transplant_counter}"]
    ):
        incoming_transplant = create_incoming_transplant(row, transplant_counter)
        transplant_ids.append(incoming_transplant.registration_id)

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

                match_row = make_transplant_match_row(
                    match_type, incoming_transplant, existing_transplant
                )

                output_dfs["updated_transplant"] = add_df_row(
                    output_dfs["updated_transplant"], match_row
                )

                existing_transplant = update_nhsbt_transplant(
                    incoming_transplant, existing_transplant
                )

                # session.commit()
        elif len(results) == 0:
            log.info(f"Adding transplant {incoming_transplant.registration_id}")

            match_type = "New"

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

    return transplant_ids


def nhsbt_import(input_file_path, audit_file_path, session, log):
    # TODO: The first step is cleaning this file with Notepad++ but I'm sure we could do this in code
    nhsbt_df = pd.read_csv(input_file_path)
    output_dfs = create_output_dfs(df_columns)
    transplant_ids = []

    for index, row in nhsbt_df.iterrows():
        index += 1
        log.info(f"on line {index + 1}")
        if match_type := import_patient(index, row, session, output_dfs, log):
            transplant_ids.extend(import_transplants(row, session, output_dfs, log))

    file_uktssas = nhsbt_df["UKTR_ID"].tolist()

    if missing_uktssa := check_missing_patients(session, file_uktssas):
        # output_dfs["missing_patients"] = create_df("missing_patients", df_columns)
        missing_patients = (
            session.query(UKT_Patient)
            .filter(UKT_Patient.uktssa_no.in_(missing_uktssa))
            .all()
        )
        patient_data = [
            missing_patient.__dict__ for missing_patient in missing_patients
        ]
        output_dfs["missing_patients"] = pd.DataFrame.from_records(patient_data)

    if missing_transplants_ids := check_missing_transplants(session, transplant_ids):
        missing_transplants = (
            session.query(UKT_Transplant)
            .filter(UKT_Transplant.registration_id.in_(missing_transplants_ids))
            .all()
        )
        transplant_data = [
            missing_transplant.__dict__ for missing_transplant in missing_transplants
        ]
        output_dfs["missing_transplants"] = pd.DataFrame.from_records(output_dfs)

    if deleted_uktssa := deleted_patient_check(session, file_uktssas):
        deleted_patients = (
            session.query(UKRR_Deleted_Patient)
            .filter(UKRR_Deleted_Patient.uktssa_no.in_(deleted_uktssa))
            .all()
        )
        deleted_data = [
            deleted_patient.__dict__ for deleted_patient in deleted_patients
        ]
        output_dfs["deleted_patients"] = pd.DataFrame.from_records(deleted_data)

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
