import os
import sys
from datetime import timedelta
from difflib import get_close_matches
from timeit import default_timer as timer
from typing import Union

import numpy as np
import pandas as pd
from rr_common.nhs_numbers import RR_Validate_NHS_No
from rr_connection_manager.classes.sql_server_connection import SQLServerConnection
from ukrdc.cohort_extract.csv_or_xlsx import extract_cohort_from_csv_or_xlsx

from rr_ukt_import.queries import new_identifier_query, postcode_query
from rr_ukt_import.utils import (
    NHSBT_ALPHANUM_LIST,
    RR_ALPHANUM_LIST,
    RR_COLUMNS,
    create_args,
    create_log,
)

PAEDS_CSV = "1 Complete Database.csv"
COVID_CSV = "covid_file_fixed.csv"


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


def investigate_nhs_numbers(digits: str) -> int:
    """
    Checks first to make sure the string provided contains only numbers then
    checks to see if the number provided is a NHS, CHI or HSC number

    Args:
        digits (str): the supposed national identifier. This should be a string
        containing only numbers but that can not be guaranteed

    Returns:
        int: This int will represent the type of national identifier.
        0 = not valid, 1 = NHS, 3 = CHI, and 4 = HSC
    """
    # Note: UKT put NHS no and CHI no in the same column
    try:
        nhs_no_to_check = int(digits)
    except Exception:
        nhs_no_to_check = 0

    if nhs_no_to_check:
        try:
            return RR_Validate_NHS_No(nhs_no_to_check)

        except ValueError as v:
            log.critical(f'Invalid NHS No: "{nhs_no_to_check}"')
    return nhs_no_to_check


def merge_df(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    left_match: Union[list, str],
    right_match: Union[list, str],
    how: str = "inner",
) -> pd.DataFrame:
    """
    Merges two dataframes. Remove blank entries in the respective columns first. Join
    inner unless otherwise specified.

    Args:
        df1 (pd.DataFrame): pandas dataframe
        df2 (pd.DataFrame): pandas dataframe
        left_match (list or str): _description_
        right_match (list or str): _description_
        how (str, optional): _description_. Defaults to "inner".

    Returns:
        pd.DataFrame: the combined dataframes
    """
    df1 = df1.dropna(subset=left_match)
    df2 = df2.dropna(subset=right_match)
    return df1.merge(df2, left_on=left_match, right_on=right_match, how=how)


def populate_rr_no_postcode_map() -> dict:
    """
    Build mapping between RR no and latest postcode

    Returns:
        dict: key = RR_ID value = Postcode
    """

    conn.session.execute(postcode_query)
    return dict(conn.session.fetchall())


def extract_chi_and_hsc_from_nhs(df: pd.DataFrame) -> pd.DataFrame:
    """
    NHSBT sent NHS, CHI and HSC in the same column. This splits
    them into separate columns.

    Args:
        df (pd.Dataframe): NHSBT patients

    Returns:
        pd.Dataframe: NHSBT patients with new columns for CHI and HSC
    """
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


def cast_df(df: pd.DataFrame, cast_list: list) -> pd.DataFrame:
    """
    Cast all the columns because of pandas weirdness. Think it was
    something to do with the mixed types in a column.

    Args:
        df (pd.DataFrame): A dataframe
        cast_list (list): List of columns to be cast

    Returns:
        pd.Dataframe: Dataframe with columns all cast to the same types
    """
    # TODO: [NHSBT-1] Deal with dd-mmm-YY format sent by NHSBT. Currently done manually
    for column in df.columns:
        if "DOB" in column or "dod" in column:
            df[column] = pd.to_datetime(df[column], errors="ignore", format="%d/%m/%Y")
        elif column in cast_list:
            df[column] = df[column].astype(object)
            df[column] = df[column].str.upper()
        else:
            df[column] = df[column].astype(np.float64)

    return df


def get_nhsbt_df(file_path) -> pd.DataFrame:
    """
    Load the NHSBT files and extract the CHI and HSC numbers into separate columns

    Args:
        file_path (str): Path to the the NHSBT file

    Returns:
        pd.Dataframe: NHSBT file as dataframe with added columns
    """
    nhsbt_df = pd.read_csv(file_path, encoding="latin-1")
    nhsbt_df = extract_chi_and_hsc_from_nhs(nhsbt_df)

    return nhsbt_df


def remove_unwanted_deleted(matched_df) -> pd.DataFrame:
    """
    Removes any entries that are taken from the deleted patients if there
    is already an entry for that patients in the normal tables.

    This acts
    as a check to make sure no patients were delete in error.

    Args:
        matched_df (pd.Dataframe): dataframe of possible matches

    Returns:
        pd.Dataframe: dataframe of possible matches minus unwanted deleted patients
    """
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

    if not deleted_df.empty:
        matched_df = pd.concat([matched_df, deleted_df])

    matched_df = matched_df.sort_values(by=["UKTR_ID"])
    return matched_df.reset_index(drop=True)


def remove_duplicates(matched_df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses various methods to remove duplicate matches.

    Args:
        matched_df (pd.DataFrame): A dataframe containing all possible matches

    Returns:
        pd.DataFrame: Dataframe with one row per patient
    """

    matched_df = matched_df.drop_duplicates(
        subset=[
            "UKTR_RR_ID",
            "UKTR_RSURNAME",
            "UKTR_RFORENAME",
            "UKTR_RDOB",
            "RR_ID",
            "RR_DOB",
            "RR_NHS_NO",
            "chi_no_x",
            "hsc_no_x",
        ],
        keep="first",
    )

    duplicate_df = matched_df[matched_df.UKTR_ID.duplicated(keep=False)]
    unique_df = matched_df.drop_duplicates(subset=["UKTR_ID"], keep=False)
    duplicate_df = match_score(duplicate_df)

    duplicate_df = deduplicate_on_score(duplicate_df)

    matched_df = matched_df.drop_duplicates(
        subset=["RR_ID", "RR_DOB", "RR_NHS_NO", "chi_no_x", "hsc_no_x"], keep="first"
    )

    matched_df = pd.concat([unique_df, duplicate_df])

    matched_df = matched_df.sort_values(by=["UKTR_ID"])
    return matched_df.reset_index(drop=True)


def add_postcodes(df, postcode_map):
    # TODO: [NHSBT-2] this might be quicker with Series.map()
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.map.html

    for index, row in df.iterrows():
        rr_number = row["RR_ID"]
        if rr_number in postcode_map.keys():
            df.loc[index, "RR_POSTCODE"] = postcode_map[rr_number]
    return df


def run_match(
    registry_patients_df: pd.DataFrame, nhsbt_patients_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Uses a range of criteria to make matches between NHSBT patients and RR patients. The
    idea here is to return as many different types of match as possible and then removing duplicates.
    This seem to be more efficient then trying to iterate through individual rows and picking
    out specific matches. Fuzzy and double barrel matching grab a few fringe case where there is
    no match on NHS number and the demo match misses because of slight differences.

    Args:
        registry_patients_df (pd.DataFrame): Registry patients
        nhsbt_patients_df (pd.DataFrame): NHSBT patients

    Returns:
        pd.DataFrame: Matched patients with one row per patient
    """

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
        nhsbt_patients_df, registry_patients_df, "UKTR_ID", "UKTSSA_NO"
    )

    rr_no_match_df = merge_df(
        nhsbt_patients_df, registry_patients_df, "UKTR_RR_ID", "RR_ID"
    )

    demographics_match_df = demo_matching(nhsbt_patients_df, registry_patients_df)

    matched_df = pd.concat(
        [
            nhs_no_match_df,
            chi_no_match_df,
            hsc_no_match_df,
            uktssa_no_match_df,
            rr_no_match_df,
            demographics_match_df,
        ]
    )

    # reduced the data set by getting only unmatched nhsbt patients
    unmatched_nhsbt_df = nhsbt_patients_df[
        (~nhsbt_patients_df["UKTR_ID"].isin(matched_df["UKTR_ID"]))
    ]

    fuzzy_match_df = fuzzy_demo_match(unmatched_nhsbt_df, registry_patients_df)

    double_barrel_match_df = double_barrel_match(
        unmatched_nhsbt_df, registry_patients_df
    )

    fringe_case_nhsbt_df = pd.concat([fuzzy_match_df, double_barrel_match_df])

    if not fringe_case_nhsbt_df.empty:
        matched_df = pd.concat([matched_df, fringe_case_nhsbt_df])

    matched_df = remove_unwanted_deleted(matched_df)
    matched_df = remove_duplicates(matched_df)

    # This should fill our NHS number with CHI and HSC numbers where present
    # preference is NHS > CHI > HSC which might need changing depending on Scottish patients
    matched_df.RR_NHS_NO.fillna(matched_df.chi_no_x, inplace=True)
    matched_df.RR_NHS_NO.fillna(matched_df.hsc_no_x, inplace=True)

    # Back fill RR forenames and surnames with the original name to preserve them
    matched_df["RR_FORENAME"] = matched_df["original_rr_forename_x"]
    matched_df["RR_SURNAME"] = matched_df["original_rr_surname_x"]
    matched_df["UKTR_RFORENAME"] = matched_df["original_nhsbt_forename_x"]
    matched_df["UKTR_RSURNAME"] = matched_df["original_nhsbt_surname_x"]

    # Remove any excess columns
    matched_df = matched_df.loc[:, ~matched_df.columns.str.contains("_x", case=False)]

    return matched_df.sort_values(by=["UKTR_ID"])


def demo_matching(nhsbt_df, rr_df):
    demographics_match_df = merge_df(
        nhsbt_df,
        rr_df,
        ["UKTR_RDOB", "UKTR_RFORENAME", "UKTR_RSURNAME"],
        ["RR_DOB", "RR_FORENAME", "RR_SURNAME"],
    )

    postcode_surname_match_df = merge_df(
        nhsbt_df,
        rr_df,
        ["UKTR_RDOB", "UKTR_RSURNAME", "UKTR_RPOSTCODE"],
        ["RR_DOB", "RR_SURNAME", "RR_POSTCODE"],
    )

    postcode_forename_match_df = merge_df(
        nhsbt_df,
        rr_df,
        ["UKTR_RDOB", "UKTR_RFORENAME", "UKTR_RPOSTCODE"],
        ["RR_DOB", "RR_FORENAME", "RR_POSTCODE"],
    )

    return pd.concat(
        [demographics_match_df, postcode_surname_match_df, postcode_forename_match_df]
    )


def fuzzy_demo_match(nhsbt_df: pd.DataFrame, rr_df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes the names (fore and sur) from the nhsbt data and looks for n closest matches in the rr_patients.
    Currently n=5 however this can be tweaked if required in the get_close_matches function. This function
    comes from difflib https://docs.python.org/3/library/difflib.html

    Please note that this is very expensive in comparison with the rest of the script and only nets a very
    fringe section of non matches.

    Args:
        nhsbt_df (pd.DataFrame): nhsbt patients
        rr_df (pd.DataFrame): rr patients

    Returns:
        pd.DataFrame: a new dataframe with all nhsbt patient close matches
    """
    # TODO: [NHSBT-3] Improved the speed if possible
    # Fuzzy matching is slow and nets only a couple of extra matches

    log.info("Starting fuzzy matching...")

    rr_df = rr_df.dropna(subset=["RR_FORENAME", "RR_SURNAME"])
    series_list = []

    for _, row in nhsbt_df.iterrows():
        if not pd.isna(row["UKTR_RSURNAME"]):
            fuzzy_surname = row["UKTR_RSURNAME"]
            surname_first_letter = fuzzy_surname[0]
            surnames = rr_df.loc[
                rr_df["RR_SURNAME"].str.startswith(surname_first_letter)
            ]["RR_SURNAME"].drop_duplicates()
            fuzzy_surname_matches = get_close_matches(fuzzy_surname, surnames, n=5)

        for match in fuzzy_surname_matches:
            fuzzy_row = row.copy()
            fuzzy_row["UKTR_RSURNAME"] = match
            series_list.append(fuzzy_row)

        if not pd.isna(row["UKTR_RFORENAME"]):
            fuzzy_forename = row["UKTR_RFORENAME"]
            forename_first_letter = fuzzy_forename[0]
            forenames = rr_df.loc[
                rr_df["RR_FORENAME"].str.startswith(forename_first_letter)
            ]["RR_FORENAME"].drop_duplicates()
            fuzzy_forename_matches = get_close_matches(fuzzy_forename, forenames, n=5)

        for match in fuzzy_forename_matches:
            fuzzy_row = row.copy()
            row["UKTR_RFORENAME"] = match
            series_list.append(fuzzy_row)

        nhsbt_df = pd.concat(series_list, axis=1).T

    return demo_matching(nhsbt_df, rr_df)


def double_barrel_match(nhsbt_df: pd.DataFrame, rr_df: pd.DataFrame) -> pd.DataFrame:
    """
    Looks at both data sets, finds names that contain white space, and assumes that indicates a double
    barrel name. It then captures the first part of the name string and uses that to generate new possible
    combos of names for matching.

    Args:
        nhsbt_df (pd.DataFrame): NHSBT patients
        rr_df (pd.DataFrame): rr_patients

    Returns:
        pd.DataFrame: double barrels match dataframe
    """
    nhsbt_forename_double_df = nhsbt_df[
        nhsbt_df["UKTR_RFORENAME"].str.contains(" ", na=False)
    ]

    split_forename_df = (
        nhsbt_forename_double_df["UKTR_RFORENAME"].str.split().str[0].to_frame()
    )

    nhsbt_forename_double_df["UKTR_RFORENAME"] = split_forename_df["UKTR_RFORENAME"]

    nhsbt_surname_double_df = nhsbt_df[
        nhsbt_df["UKTR_RSURNAME"].str.contains(" ", na=False)
    ]

    split_surname_df = (
        nhsbt_surname_double_df["UKTR_RSURNAME"].str.split().str[0].to_frame()
    )

    nhsbt_surname_double_df["UKTR_RSURNAME"] = split_surname_df["UKTR_RSURNAME"]

    nhsbt_both_df = nhsbt_forename_double_df.copy()
    nhsbt_both_df["UKTR_RSURNAME"] = split_surname_df["UKTR_RSURNAME"]
    nhsbt_both_df["UKTR_RFORENAME"] = split_forename_df["UKTR_RFORENAME"]

    # TODO: [NHSBT-4] Repeating code is bad

    rr_forename_double_df = rr_df[rr_df["RR_FORENAME"].str.contains(" ", na=False)]
    split_forename_df = (
        rr_forename_double_df["RR_FORENAME"].str.split().str[0].to_frame()
    )
    rr_forename_double_df["RR_FORENAME"] = split_forename_df["RR_FORENAME"]

    rr_surname_double_df = rr_df[rr_df["RR_SURNAME"].str.contains(" ", na=False)]
    split_surname_df = rr_surname_double_df["RR_SURNAME"].str.split().str[0].to_frame()
    rr_surname_double_df["RR_SURNAME"] = split_surname_df["RR_SURNAME"]

    nhsbt_df = pd.concat(
        [nhsbt_df, nhsbt_forename_double_df, nhsbt_surname_double_df, nhsbt_both_df]
    )

    rr_df = pd.concat([rr_df, rr_forename_double_df, rr_surname_double_df])

    return demo_matching(nhsbt_df, rr_df)


def match_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Where duplicate rows exist, assign a score that can be use to pick the best
    possible match. National identifiers > RR_ID > NHSBT_ID > demographics

    Args:
        df (pd.DataFrame): Matched patients with more than one matching row

    Returns:
        pd.DataFrame: Matched patients with a new column for matching score
    """
    row_scores = []
    for _, row in df.iterrows():
        score = 0
        if row["UKTR_RNHS_NO"] == row["RR_NHS_NO"]:
            score += 50

        if row["UKTR_RR_ID"] == row["RR_ID"]:
            score += 30

        if row["UKTR_ID"] == row["UKTSSA_NO"]:
            score += 10

        if row["UKTR_RDOB"] == row["RR_DOB"]:
            score += 5

        if (
            row["UKTR_RSURNAME"] == row["RR_SURNAME"]
            and row["UKTR_RFORENAME"] == row["RR_FORENAME"]
        ):
            score += 3

        if (
            row["UKTR_RSURNAME"] == row["RR_SURNAME"]
            or row["UKTR_RFORENAME"] == row["RR_FORENAME"]
        ):
            score + 1

        row_scores.append(score)
    df["Score_x"] = row_scores
    return df


def deduplicate_on_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates based on the matching score. Score assigned according to
    National identifiers > RR_ID > NHSBT_ID > demographics

    Args:
        df (pd.DataFrame): Matched patient records with more than one match

    Returns:
        pd.DataFrame: Matched patients but only the best matches.
    """
    checked = []
    to_keep = []

    for index, row in df.iterrows():
        patient = row["UKTR_ID"]
        if patient in checked:
            continue
        checked.append(patient)
        duplicates_df = df[
            df["UKTR_ID"].isin(
                [
                    patient,
                ]
            )
        ]

        best_fit = 0
        best_fit_score = 0

        # For this to work you need to ensure the index persists from the main DF
        for _, dup_row in duplicates_df.iterrows():
            if dup_row["Score_x"] > best_fit_score:
                best_fit_score = dup_row["Score_x"]
                best_fit = index

        to_keep.append(best_fit)

    return df[df.index.isin(to_keep)]


def main():
    ### ----- Check files ----- ###
    log.info("checking file paths...")
    input_file_path = build_and_check_file_path(args.root, f"UKTR_DATA_{args.date}.csv")
    paeds_file_path = build_and_check_file_path(args.root, PAEDS_CSV)
    covid_file_path = build_and_check_file_path(args.root, COVID_CSV)

    ### ----- Set output file ----- ###
    output_filename = os.path.join(args.root, f"UKTR_DATA_{args.date}_MATCHED_test.csv")

    ### ----- Build raw patient dataframes ----- ###
    log.info("collecting patients...")
    # TODO: [NHSBT-5] This works but causes a warning message.
    rr_df = pd.read_sql(new_identifier_query, conn.connection)
    rr_df.columns = RR_COLUMNS + [
        "deleted_x",
    ]

    paeds_df = pd.DataFrame(extract_cohort_from_csv_or_xlsx(paeds_file_path))
    paeds_df.columns = RR_COLUMNS

    covid_df = pd.DataFrame(extract_cohort_from_csv_or_xlsx(covid_file_path))
    covid_df.columns = RR_COLUMNS

    registry_patients_df = pd.concat([rr_df, paeds_df, covid_df])
    registry_patients_df = cast_df(registry_patients_df, RR_ALPHANUM_LIST)

    nhsbt_patients_df = get_nhsbt_df(input_file_path)
    nhsbt_patients_df = cast_df(nhsbt_patients_df, NHSBT_ALPHANUM_LIST)

    # To maintain original names after fuzzy and double barrel matching
    registry_patients_df["original_rr_forename_x"] = registry_patients_df["RR_FORENAME"]
    registry_patients_df["original_rr_surname_x"] = registry_patients_df["RR_SURNAME"]
    nhsbt_patients_df["original_nhsbt_forename_x"] = nhsbt_patients_df["UKTR_RFORENAME"]
    nhsbt_patients_df["original_nhsbt_surname_x"] = nhsbt_patients_df["UKTR_RSURNAME"]

    ### ----- Add postcodes to RR patients ----- ###
    log.info("building postcode map...")
    postcode_map = populate_rr_no_postcode_map()
    log.info("adding postcodes...")
    registry_patients_df = add_postcodes(registry_patients_df, postcode_map)

    ### ----- Match Patients ----- ###
    log.info("matching patients...")
    matched_df = run_match(registry_patients_df, nhsbt_patients_df)

    ### ----- Write to CSV ----- ###
    log.info("writing file...")
    matched_df.to_csv(output_filename, index=False)


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    start = timer()
    conn = SQLServerConnection(app="ukrdc_rrsqllive")
    log = create_log()
    args = create_args()
    main()
    end = timer()
    log.info(f"The script completed in {timedelta(seconds=end - start)}")
