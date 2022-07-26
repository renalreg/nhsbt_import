from logging import Logger
import argparse
import logging.config
import yaml


NHSBT_ALPHANUM_LIST = ["UKTR_RSURNAME", "UKTR_RFORENAME", "UKTR_RPOSTCODE"]

RR_ALPHANUM_LIST = [
    "RR_SURNAME",
    "RR_FORENAME",
    "RR_POSTCODE",
    "bapn_no_x",
    "deleted_x",
]

# Suffix of x to allow easy deletion
RR_COLUMNS = [
    "RR_ID",
    "RR_SURNAME",
    "RR_FORENAME",
    "RR_SEX",
    "RR_DOB",
    "dod_x",
    "bapn_no_x",
    "chi_no_x",
    "RR_NHS_NO",
    "hsc_no_x",
    "UKTSSA_NO",
    "RR_POSTCODE",
]


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
