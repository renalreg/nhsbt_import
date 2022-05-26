import os
import sys
from datetime import timedelta
from timeit import default_timer as timer

import numpy as np
import pandas as pd

from rr_common.nhs_numbers import RR_Validate_NHS_No
from rr_connection_manager.classes.sql_server_connection import SQLServerConnection
from ukrdc.cohort_extract.csv_or_xlsx import extract_cohort_from_csv_or_xlsx
from rr_ukt_import.queries import identifier_query, postcode_query, new_identifier_query
from rr_ukt_import.utils import (
    create_args,
    create_log,
    NHSBT_ALPHANUM_LIST,
    RR_ALPHANUM_LIST,
    RR_COLUMNS,
)

PAEDS_CSV = "1 Complete Database.csv"


def build_and_check_file_path(root: str, file_name: str) -> str:
    """
    builds a file path str and returns it if the file exists
    otherwise kills process

    Args:
        root (str): root directory
        file_name (str): file name

    Returns:
        str: file path
    """
    file_path = os.path.join(root, file_name)
    if not os.path.exists(file_path):
        log.critical(f"{file_path} does not exist")
        sys.exit(1)
    log.info(f"importing patients from {file_name} into the db...")
    return file_path


def investigate_nhs_numbers(digits):
    # Note: UKT put NHS no and CHI no in the same column
    try:
        nhs_no_to_check = int(digits)
    except:
        nhs_no_to_check = None

    if nhs_no_to_check:
        try:
            return RR_Validate_NHS_No(int(nhs_no_to_check))

        except ValueError as v:
            log.critical(f'Invalid NHS No: "{nhs_no_to_check}"')


def merge_df(df1, df2, left_match, right_match, how="inner"):
    df1 = df1.dropna(subset=left_match)
    df2 = df2.dropna(subset=right_match)
    return df1.merge(df2, left_on=left_match, right_on=right_match, how=how)


# TODO: Use this to add more postcodes
def populate_rr_no_postcode_map():
    """Build mapping between RR no and latest postcode"""

    conn.session.execute(postcode_query)
    return dict(conn.session.fetchall())


def extract_chi_and_hsc_from_nhs(df):
    df.reset_index()
    df["UKT_CHINO_x"] = np.nan
    df["UKT_HSCNO_x"] = np.nan
    for index, row in df.iterrows():
        nhs_no_to_check = row["UKTR_RNHS_NO"]
        number_type = investigate_nhs_numbers(nhs_no_to_check)
        if number_type == 3:
            df.at[index, "UKT_CHINO_x"] = nhs_no_to_check
        if number_type == 4:
            df.at[index, "UKT_HSCNO_x"] = nhs_no_to_check
    return df


def cast_df(df, cast_list):
    for column in df.columns:
        if "DOB" in column or "dod" in column:
            df[column] = pd.to_datetime(df[column], errors="ignore", format="%d/%m/%Y")
        elif column in cast_list:
            df[column] = df[column].astype(object)
            df[column] = df[column].str.upper()
        else:
            df[column] = df[column].astype(np.float64)

    return df


def get_nhsbt_df(file_path):
    nhsbt_df = pd.read_csv(file_path, encoding="latin-1")
    nhsbt_df = extract_chi_and_hsc_from_nhs(nhsbt_df)
    nhsbt_df = cast_df(nhsbt_df, NHSBT_ALPHANUM_LIST)

    return nhsbt_df


def remove_unwanted_deleted(matched_df):
    log.info("checking deleted patients...")

    deleted_df = matched_df[matched_df.deleted_x == "Y"]
    matched_df = matched_df[matched_df.deleted_x != "Y"]

    deleted_df = pd.merge(
        deleted_df,
        matched_df,
        how="outer",
        suffixes=("", "_y"),
        indicator=True,
        on=["UKTR_ID"],
    )

    deleted_df = deleted_df[deleted_df["_merge"] == "left_only"][matched_df.columns]
    deleted_df = deleted_df.drop_duplicates(subset=["UKTR_ID"])

    matched_df = pd.concat([matched_df, deleted_df])
    matched_df = matched_df.sort_values(by=["UKTR_ID"])
    return matched_df.reset_index(drop=True)


def remove_duplicates(matched_df):
    matched_df = matched_df.drop_duplicates(
        subset=["RR_ID", "RR_DOB", "RR_NHS_NO", "chi_no_x", "hsc_no_x"], keep="first"
    )

    duplicate_df = matched_df[matched_df["RR_ID"].isnull()]
    matched_df = matched_df[~matched_df["RR_ID"].isnull()]

    duplicate_df = pd.merge(
        duplicate_df,
        matched_df,
        how="outer",
        suffixes=("", "_y"),
        indicator=True,
        on=["RR_SURNAME", "RR_FORENAME"],
    )

    duplicate_df = duplicate_df[duplicate_df["_merge"] == "left_only"][
        matched_df.columns
    ]

    matched_df = pd.concat([matched_df, duplicate_df])
    matched_df = matched_df.sort_values(by=["UKTR_ID"])
    return matched_df.reset_index(drop=True)


def run_match(registry_patients_df, nhsbt_patients_df):

    log.info("building postcode map...")
    rr_no_postcode_map = populate_rr_no_postcode_map()

    log.info("matching patients...")

    nhs_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKTR_RNHS_NO", "RR_NHS_NO"
    )
    chi_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKT_CHINO_x", "chi_no_x"
    )

    hsc_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKT_HSCNO_x", "hsc_no_x"
    )

    uktssa_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKTR_ID", "uktssa_no_x"
    )

    rr_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKTR_RR_ID", "RR_ID"
    )

    demo_match_df = merge_df(
        nhsbt_patients_df,
        registry_patients_df,
        ["UKTR_RDOB", "UKTR_RSURNAME", "UKTR_RFORENAME"],
        ["RR_DOB", "RR_SURNAME", "RR_FORENAME"],
    )

    matched_df = pd.concat(
        [
            nhs_no_match_df,
            chi_no_match_df,
            hsc_no_match_df,
            uktssa_no_match_df,
            rr_no_match_df,
            demo_match_df,
        ]
    )

    matched_df = matched_df.drop_duplicates(
        subset=["RR_ID", "RR_DOB", "RR_NHS_NO", "chi_no_x", "hsc_no_x"], keep="first"
    )

    matched_df = remove_unwanted_deleted(matched_df)
    matched_df = remove_duplicates(matched_df)

    # This should fill our NHS number with CHI and HSC numbers where present
    # preference is NHS > CHI > HSC which might need changing depending on Scottish patients
    # TODO: make sure new Scottish patients have CHI numbers
    matched_df.RR_NHS_NO.fillna(matched_df.chi_no_x, inplace=True)
    matched_df.RR_NHS_NO.fillna(matched_df.hsc_no_x, inplace=True)
    # Remove any excess columns
    matched_df = matched_df.loc[:, ~matched_df.columns.str.contains("_x", case=False)]

    return matched_df.sort_values(by=["UKTR_ID"])


def main():
    ### ----- Check files ----- ###
    log.info("checking file paths...")
    input_file_path = build_and_check_file_path(args.root, f"UKTR_DATA_{args.date}.csv")
    paeds_file_path = build_and_check_file_path(args.root, PAEDS_CSV)
    ### ----- Set output file ----- ###
    output_filename = os.path.join(args.root, f"UKTR_DATA_{args.date}_MATCHED_test.csv")
    ### ----- Build dataframes ----- ###
    # TODO: This isn't getting patients from deleted table
    log.info("collecting patients...")
    rr_df = pd.read_sql(new_identifier_query, conn.connection)
    rr_df.columns = RR_COLUMNS + ["deleted_x"]

    paeds_df = pd.DataFrame(extract_cohort_from_csv_or_xlsx(paeds_file_path))
    paeds_df.columns = RR_COLUMNS

    registry_patients_df = pd.concat([paeds_df, rr_df])
    registry_patients_df = cast_df(registry_patients_df, RR_ALPHANUM_LIST)

    nhsbt_patients_df = get_nhsbt_df(input_file_path)

    matched_df = run_match(registry_patients_df, nhsbt_patients_df)
    log.info("writing file...")
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
