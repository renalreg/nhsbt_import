from logging import Logger
import argparse
import logging.config
import yaml
import pandas as pd
from typing import List

WHITELIST = {
    "renalregnumber": "rrno",
    "rrnum": "rrno",
    "lastname": "surname",
    "familyname": "surname",
    "firstname": "surname",
    "givenname": "surname",
    "gender": "sex",
    "dateofbirth": "dob",
    "birthdate": "dob",
    "dateofdeath": "dod",
    "deathdate": "dod",
}


def create_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ukt_match")
    parser.add_argument("--root", type=str, help="Specify Root Folder", required=True)
    parser.add_argument("--date", type=str, help="ddMMMYYY")
    parser.add_argument("--output", type=str, help="Specify alternate output")
    return parser.parse_args()


def create_log() -> Logger:
    logging.config.dictConfig(
        yaml.load(open("logconf.yaml", "r"), Loader=yaml.SafeLoader)
    )
    return logging.getLogger("ukt_match")


def load_file(file):
    """
    Load a file into a dataframe if it is a csv or xlsx file

    Args:
        file (str): A file path

    Raises:
        RuntimeError: Raised if file doesn't have the correct extension

    Returns:
        Dataframe: The file as a dataframe
    """
    if file.endswith(".csv"):
        # Had some issues with encoding so have included an option that seems to have fixed it
        return pd.read_csv(file, encoding="latin-1")
    elif file.endswith(".xlsx"):
        return pd.read_excel(file)
    else:
        raise RuntimeError("File extension not recognized")


def clean_headers(paeds_df):
    """
    Attempt to normalize the headers

    Args:
        paeds_df (Dataframe): Peads data frame taken from a file

    Returns:
        List: List of headers that have been stripped of white space, underscores and
        are all lower case.
    """
    cleaned_headers: List[str] = []
    columns = tuple(paeds_df.columns)

    for col in columns:
        clean_header = col.strip().lower().replace("_", " ").replace(" ", "")
        if clean_header in WHITELIST:
            clean_header = WHITELIST[clean_header]
        cleaned_headers.append(clean_header)
    return cleaned_headers
