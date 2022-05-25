# import csv
import os
from wsgiref import headers
import numpy as np
import pandas as pd

# import sys

from datetime import timedelta
from timeit import default_timer as timer
from rr_connection_manager.classes.sql_server_connection import SQLServerConnection
from sqlalchemy import extract

from ukrdc.cohort_extract.csv_or_xlsx import extract_cohort_from_csv_or_xlsx, load_file
from rr_ukt_import.utils import create_args, create_log

# # from datetime import datetime
# # import logging
# # import time
from rr_common.general_exceptions import Error

from rr_ukt_import.queries import postcode_query, identifier_query, patients_query

from rr_common.nhs_numbers import RR_Validate_NHS_No

# # from rr_common.rr_general_utils import rr_str

# from rr_ukt_import.ukrr import process

# from rr_ukt_import import ukrr
# from rr_ukt_import.dateutils import convert_datetime_string_to_datetime


PAEDS_CSV = "1 Complete Database.csv"

# PROCESS_Q100 = True

UKT_COLUMNS = [
    "UKTR_RR_ID",
    "UKTR_ID",
    "UKTR_TX_ID1",
    "UKTR_TX_ID2",
    "UKTR_TX_ID3",
    "UKTR_TX_ID4",
    "UKTR_TX_ID5",
    "UKTR_TX_ID6",
    "PREVIOUS_MATCH",
    "UKTR_RSURNAME",
    "UKTR_RFORENAME",
    "UKTR_RDOB",
    "UKTR_RSEX",
    "UKTR_RPOSTCODE",
    "UKTR_RNHS_NO",
]
RR_COLUMNS = [
    "RR_ID",
    "RR_SURNAME",
    "RR_FORENAME",
    "RR_DOB",
    "RR_SEX",
    "RR_POSTCODE",
    "RR_NHS_NO",
]

RENAME_COLUMNS = {
    "UKTR_ID": "uktssa_no",
    "UKTR_RR_ID": "rr_no",
    "UKTR_RSURNAME": "surname",
    "UKTR_RFORENAME": "forename",
    "UKTR_RDOB": "dob",
    "UKTR_RSEX": "sex",
    "UKTR_RPOSTCODE": "postcode",
    "UKTR_RNHS_NO": "nhs_no",
}

DF_COLUMNS = [
    "rr_no",
    "surname",
    "forename",
    "sex",
    "dob",
    "dod",
    "bapn_no",
    "chi_no",
    "nhs_no",
    "hsc_no",
    "uktssa_no",
    "postcode",
]

# TODO: Leaving here in case it needs to be written back in but it needs to
# # TODO: handled by the cohort extract which probably needs some work
# def import_q100(db, rr_no_postcode_map):
#     """Import Q100 patients into a temporary table"""

#     # process return a list of all patients found in Q100 files
#     # (rr_no, surname, forename, sex, dob, local_hosp_no, chi_no, nhs_no, hsc_no)

#     q100_patients = ukrr.process()

#     dummy_rr_no = 888800001

#     for line_no, row in enumerate(q100_patients, start=1):
#         # Ignore ones with an RR_No as these will be the RenalReg DB
#         if row[0] in ("", None):

#             local_hosp_no = row[5]
#             nhs_no = row[7]
#             chi_no = row[6]
#             hsc_no = row[8]

#             rr_no = dummy_rr_no
#             dummy_rr_no += 1

#             uktssa_no = None
#             surname = row[1]
#             forename = row[2]
#             dob = row[4]
#             sex = row[3]

#             patients_sql = """
#                 INSERT INTO #UKT_MATCH_PATIENTS (
#                     UNDELETED_RR_NO,
#                     RR_NO,
#                     UKTSSA_NO,
#                     SURNAME,
#                     FORENAME,
#                     DATE_BIRTH,
#                     NEW_NHS_NO,
#                     CHI_NO,
#                     HSC_NO,
#                     LOCAL_HOSP_NO,
#                     SOUNDEX_SURNAME,
#                     SOUNDEX_FORENAME,
#                     PATIENT_TYPE
#                 )
#                 VALUES (
#                     :RR_NO,
#                     :RR_NO,
#                     :UKTSSA_NO,
#                     :SURNAME,
#                     :FORENAME,
#                     :DATE_BIRTH,
#                     :NEW_NHS_NO,
#                     :CHI_NO,
#                     :HSC_NO,
#                     :LOCAL_HOSP_NO,
#                     SOUNDEX(dbo.normalise_surname2(:SURNAME)),
#                     SOUNDEX(dbo.normalise_forename2(:FORENAME)),
#                     'Q100'
#                 )
#             """

#             db.execute(
#                 patients_sql,
#                 {
#                     "UNDELETED_RR_NO": rr_no,
#                     "NEW_NHS_NO": nhs_no,
#                     "CHI_NO": chi_no,
#                     "HSC_NO": hsc_no,
#                     "RR_NO": rr_no,
#                     "UKTSSA_NO": uktssa_no,
#                     "SURNAME": surname,
#                     "FORENAME": forename,
#                     "DATE_BIRTH": dob,
#                     "SEX": sex,
#                     "LOCAL_HOSP_NO": local_hosp_no,
#                 },
#             )


def match_patient(
    db,
    log,
    row,
    nhs_no_map,
    chi_no_map,
    hsc_no_map,
    uktssa_no_map,
    rr_no_postcode_map,
    rr_no_map,
):

    # A single patient matched
    if len(matched_rr_nos) == 1:
        rr_row = identifier_matches[0]

        rr_no = rr_row[0]

        # Format DOB
        rr_row[3] = rr_str(rr_row[3])

        # Populate postcode
        rr_row[5] = rr_no_postcode_map.get(rr_no, None)

        row.extend(rr_row)

        print("NHS Lookup Match")

    else:
        rr_no = None
        hosp_centre = None
        local_hosp_no = None
        scot_reg_no = None
        rr_only = "Y"
        include_deleted = "Y"

        surname = row[9]
        forename = row[10]
        postcode = row[13]

        dob = row[11]

        if dob != "":
            dob_to_convert = dob
            dob = convert_datetime_string_to_datetime(dob)
            if not dob:
                log.critical(
                    ("no date-time conversion" f" for date-of-birth {dob_to_convert}")
                )
            else:
                log.debug(f"Convert {dob_to_convert} to {dob}")
        else:
            dob = None

        params = [
            surname,
            forename,
            dob,
            rr_no,
            nhs_no,
            chi_no,
            hsc_no,
            uktssa_no,
            hosp_centre,
            local_hosp_no,
            scot_reg_no,
            postcode,
            rr_only,
            include_deleted,
        ]
        db.cursor.callproc("PROC_UKT_MATCH_PATIENT_MATCHING", params)
        # Found a match
        for result in db.cursor:
            rr_no = result[0]
            surname = result[3]
            forename = result[2]
            dob = rr_str(result[4])
            sex = get_patient_sex(db, result[0])
            nhs_no = result[5]

            # Postcode will be missing from the result
            # as we aren't supplying a value for hosp_centre and this is
            # used to join the residency table
            postcode = rr_no_postcode_map.get(rr_no, "")
            row.extend([rr_no, surname, forename, dob, sex, postcode, nhs_no])
            break

    # Ensure correct number of output columns
    pad_row(row, len(UKT_COLUMNS + RR_COLUMNS))

    prev_match_rr_no = None
    if row[0] is not None:
        try:
            prev_match_rr_no = int(row[0])
        except ValueError:
            pass
    match_rr_no = None
    if row[15] is not None:
        try:
            match_rr_no = int(row[15])
        except ValueError:
            pass
    if prev_match_rr_no is None:
        # Didn't match last time
        prev_match = 0
        if match_rr_no:
            # But matched this time
            log.info(f"NEW_MATCH: {uktssa_no} RR_NO={match_rr_no}")
    elif match_rr_no is None:
        # Didn't match this time
        prev_match = 3
        log.info(f"USED_TO_MATCH: {uktssa_no} PREV_RR_NO={prev_match_rr_no}")
    elif prev_match_rr_no == match_rr_no:
        # Matched to the same patient
        prev_match = 1
        log.info(f"Matched {uktssa_no} PREV_RR_NO={prev_match_rr_no} {match_rr_no}")
    else:
        # Matched to a different patient
        prev_match = 2
        m = f"DIFFERENT_MATCH: {uktssa_no} PREV_RR_NO={prev_match_rr_no} {match_rr_no}"
        log.info(m)

    row[8] = prev_match

    return row


def investigate_nhs_numbers(digits):
    # Note: UKT put NHS no and CHI no in the same column
    try:
        nhs_no_to_check = int(digits)
    except:
        nhs_no_to_check = None

    nhs_no = None
    chi_no = None
    hsc_no = None

    if nhs_no_to_check:
        try:
            return RR_Validate_NHS_No(int(nhs_no_to_check))

        except ValueError as v:
            log.critical(f'Invalid NHS No: "{nhs_no_to_check}"')


def merge_df(df1, df2, left_match, right_match):
    df1 = df1.dropna(subset=left_match)
    df2 = df2.dropna(subset=right_match)
    return df1.merge(df2, left_on=left_match, right_on=right_match)


def run_match(registry_patients_df, nhsbt_patients_df):

    log.info("building postcode map...")
    rr_no_postcode_map = populate_rr_no_postcode_map()
    log.info("building identifier map...")

    nhs_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKTR_RNHS_NO", "nhs_no"
    )
    chi_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKT_CHINO", "chi_no"
    )

    hsc_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKT_HSCNO", "hsc_no"
    )

    uktssa_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKTR_ID", "uktssa_no"
    )

    rr_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKTR_RR_ID", "rr_no"
    )

    demo_match_df = merge_df(
        nhsbt_patients_df,
        registry_patients_df,
        ["UKTR_RDOB", "UKTR_RSURNAME", "UKTR_RFORENAME"],
        ["dob", "surname", "forename"],
    )

    df_list = [
        nhs_no_match_df,
        chi_no_match_df,
        hsc_no_match_df,
        uktssa_no_match_df,
        rr_no_match_df,
        demo_match_df,
    ]
    matched_df = pd.concat(df_list)
    matched_df = matched_df.drop_duplicates()

    log.info("matching patients...")

    columns = next(uktr_reader)
    check_columns(columns, UKT_COLUMNS)

    # Up to this point just collecting patients

    log.info("Start Matching run")
    start_run = time.clock()
    combined_columns = UKT_COLUMNS + RR_COLUMNS
    ukrr_writer.writerow(combined_columns)
    for line_number, row in enumerate(uktr_reader, start=1):
        if line_number % 1000 == 0:
            timing = line_number / (time.clock() - start_run)
            log.info("line %d (%.2f/s)" % (line_number, timing))
        row = match_patient(
            db,
            log,
            row,
            nhs_no_map,
            chi_no_map,
            hsc_no_map,
            uktssa_no_map,
            rr_no_postcode_map,
            rr_no_map,
        )
        ukrr_writer.writerow(row)
    #
    # now write out the combined columns
    log.info("Finish matching run")


def populate_rr_no_postcode_map():
    """Build mapping between RR no and latest postcode"""

    conn.session.execute(postcode_query)
    return dict(conn.session.fetchall())


def cast_df(df):
    df["nhs_no"] = df["nhs_no"].astype(np.float64)
    df["chi_no"] = df["chi_no"].astype(np.float64)
    df["hsc_no"] = df["hsc_no"].astype(np.float64)
    df["dob"] = pd.to_datetime(df["dob"])
    return df


def rename_columns(df):
    return df.rename(columns=RENAME_COLUMNS)


def extract_chi_and_hsc_from_nhs(df):
    df.reset_index()
    df["UKT_CHINO"] = np.nan
    df["UKT_HSCNO"] = np.nan
    for index, row in df.iterrows():
        nhs_no_to_check = row["UKTR_RNHS_NO"]
        number_type = investigate_nhs_numbers(nhs_no_to_check)
        if number_type == 3:
            df.at[index, "UKT_CHINO"] = nhs_no_to_check
        if number_type == 4:
            df.at[index, "UKT_HSCNO"] = nhs_no_to_check
    return df


def get_nhsbt_df(file_path):
    nhsbt_raw_df = pd.read_csv(file_path, encoding="latin-1")
    nhsbt_raw_df = extract_chi_and_hsc_from_nhs(nhsbt_raw_df)
    nhsbt_raw_df["UKTR_RNHS_NO"] = nhsbt_raw_df["UKTR_RNHS_NO"].astype(np.float64)
    nhsbt_raw_df["UKTR_RDOB"] = pd.to_datetime(nhsbt_raw_df["UKTR_RDOB"])
    nhsbt_raw_df["UKT_CHINO"] = nhsbt_raw_df["UKT_CHINO"].astype(np.float64)
    nhsbt_raw_df["UKT_HSCNO"] = nhsbt_raw_df["UKT_HSCNO"].astype(np.float64)

    return nhsbt_raw_df


def build_and_check_file_path(root: str, file_name: str) -> str:
    """
        builds a file path str and returns it if the file exists
        otherwise kills process

    #     Args:
    #         root (str): root directory
    #         file_name (str): file name

    #     Returns:
    #         str: file path
    #"""
    file_path = os.path.join(root, file_name)
    if not os.path.exists(file_path):
        log.critical(f"{file_path} does not exist")
        sys.exit(1)
    log.info(f"importing patients from {file_name} into the db...")
    return file_path


def main():
    ### ----- Check files ----- ###
    input_file_path = build_and_check_file_path(args.root, f"UKTR_DATA_{args.date}.csv")
    paeds_file_path = build_and_check_file_path(args.root, PAEDS_CSV)
    ### ----- Set output file ----- ###
    output_filename = os.path.join(args.root, f"UKTR_DATA_{args.date}_MATCHED.csv")
    ### ----- Build dataframes ----- ###
    # TODO: This isn't getting patients from deleted table
    rr_df = pd.read_sql(identifier_query, conn.connection)
    rr_df.columns = DF_COLUMNS

    paeds_df = pd.DataFrame(extract_cohort_from_csv_or_xlsx(paeds_file_path))
    paeds_df.columns = DF_COLUMNS

    registry_patients_df = pd.concat([paeds_df, rr_df])
    registry_patients_df = cast_df(registry_patients_df)

    nhsbt_patients_df = get_nhsbt_df(input_file_path)

    matched_df = run_match(registry_patients_df, nhsbt_patients_df)
    matched_df.to_csv(output_filename, index=False)


if __name__ == "__main__":
    start = timer()
    conn = SQLServerConnection(app="ukrdc_rrsqllive")
    print(conn.connection_check())
    log = create_log()
    args = create_args()
    main()
    end = timer()
    print(timedelta(seconds=end - start))
