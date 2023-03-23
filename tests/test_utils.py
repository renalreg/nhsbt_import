import logging

import pandas as pd
import pytest
from faker import Faker
from unittest.mock import MagicMock
import os

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

from ukrr_models.nhsbt_models import UKT_Patient

from nhsbt_import.utils import (
    args_parse,
    create_df,
    create_incoming_patient,
    create_logs,
    create_session,
    get_error_file_path,
    update_nhsbt_patient,
    make_patient_match_row,
)

fake = Faker()


@pytest.fixture
def incoming_patient():
    return UKT_Patient(
        uktssa_no=fake.random_number(digits=6),
        surname=fake.last_name(),
        forename=fake.first_name(),
        sex=fake.random_element(elements=("M", "F")),
        post_code=fake.postcode(),
        new_nhs_no=fake.random_number(digits=10),
        chi_no=None,
        hsc_no=None,
        rr_no=None,
        ukt_date_death=None,
        ukt_date_birth=None,
    )


@pytest.fixture
def existing_patient():
    return UKT_Patient(
        uktssa_no=fake.random_number(digits=6),
        surname=fake.last_name(),
        forename=fake.first_name(),
        sex=fake.random_element(elements=("M", "F")),
        post_code=fake.postcode(),
        new_nhs_no=fake.random_number(digits=10),
        chi_no=None,
        hsc_no=None,
        rr_no=fake.uuid4(),
        ukt_date_death=None,
        ukt_date_birth=None,
    )


def test_args_parse():
    args, help_text = args_parse(["--input_file", fake.file_path(depth=3)])

    # Assert that the parser has the correct argument
    assert isinstance(args.input_file, str)

    # Assert that the help text is not empty
    assert help_text.strip() != ""

    # Assert that the help text includes the argument description
    assert "-if INPUT_FILE, --input_file INPUT_FILE" in help_text


def test_create_df():
    name = "test"
    df_columns = {"test": fake.pylist()}

    # Call the function to create a dataframe
    df = create_df(name, df_columns)

    # Assert that the dataframe has the correct columns
    expected_columns = df_columns[name]
    assert list(df.columns) == expected_columns

    # Assert that the dataframe is empty
    assert df.empty


def test_create_incoming_patient():
    row = {
        "UKTR_ID": fake.random_int(),
        "UKTR_RSURNAME": fake.last_name(),
        "UKTR_RFORENAME": fake.first_name(),
        "UKTR_RSEX": fake.random_element(elements=("M", "F")),
        "UKTR_RPOSTCODE": fake.postcode(),
        "UKTR_RNHS_NO": fake.random_number(digits=10),
        "UKTR_RCHI_NO_NI": fake.random_number(digits=8),
        "UKTR_RCHI_NO_SCOT": fake.random_number(digits=10),
        "UKTR_DDATE": fake.date(),
        "UKTR_RDOB": fake.date_of_birth(),
    }

    patient = create_incoming_patient(row)

    assert patient.uktssa_no == row["UKTR_ID"]
    assert patient.surname == row["UKTR_RSURNAME"]
    assert patient.forename == row["UKTR_RFORENAME"]
    assert patient.sex == row["UKTR_RSEX"]
    assert patient.post_code == row["UKTR_RPOSTCODE"]
    assert patient.new_nhs_no == row["UKTR_RNHS_NO"]
    assert patient.chi_no == row["UKTR_RCHI_NO_NI"]
    assert patient.hsc_no == row["UKTR_RCHI_NO_SCOT"]
    assert patient.rr_no is None
    assert patient.ukt_date_death == row["UKTR_DDATE"]
    assert patient.ukt_date_birth == row["UKTR_RDOB"]


def test_create_logs(mocker):
    # Create a mock logging config dictionary
    logconf = {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "simple",
            }
        },
        "loggers": {
            "nhsbt_import": {
                "handlers": ["console"],
                "level": "DEBUG",
            }
        },
        "formatters": {
            "simple": {
                "format": "%(levelname)s %(message)s",
            }
        },
    }

    # Mock the yaml module to return the logconf dictionary
    mocker.patch("yaml.safe_load", MagicMock(return_value=logconf))

    # Call the create_logs function
    logger = create_logs()

    # Check that the logger has the correct attributes
    assert isinstance(logger, logging.Logger)
    assert logger.name == "nhsbt_import"
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    assert logger.handlers[0].level == logging.DEBUG
    assert isinstance(logger.handlers[0].formatter, logging.Formatter)
    assert logger.handlers[0].formatter._fmt == "%(levelname)s %(message)s"


def test_create_session():
    # Call the create_session function to create a SQLAlchemy Session object
    session = create_session()

    # Check that the session is an instance of the Session class
    assert isinstance(session, Session)

    # Check that the session's engine has the correct configuration
    assert isinstance(session.bind, Engine)
    assert session.bind.driver == "pyodbc"
    assert session.bind.url.host == "rr-sql-live"
    assert session.bind.url.database == "renalreg"


def test_get_error_file_path():
    # Define an input file path
    input_file = fake.file_path(depth=3)

    # Call the get_error_file_path function to create an error file path
    error_file_path = get_error_file_path(input_file)

    # Check that the error file path has the correct format
    expected_path = os.path.join(os.path.split(input_file)[0], "NHSBT_Errors.xls")
    assert error_file_path == expected_path


def test_update_nhsbt_patient(incoming_patient, existing_patient):
    # Call the update_nhsbt_patient function to update the existing patient
    updated_patient = update_nhsbt_patient(incoming_patient, existing_patient)

    # Check that the existing patient has been updated correctly
    assert updated_patient.uktssa_no == incoming_patient.uktssa_no
    assert updated_patient.surname == incoming_patient.surname
    assert updated_patient.forename == incoming_patient.forename
    assert updated_patient.sex == incoming_patient.sex
    assert updated_patient.post_code == incoming_patient.post_code
    assert updated_patient.new_nhs_no == incoming_patient.new_nhs_no
    assert updated_patient.chi_no == incoming_patient.chi_no
    assert updated_patient.hsc_no == incoming_patient.hsc_no
    assert updated_patient.rr_no == existing_patient.rr_no
    assert updated_patient.ukt_date_death == incoming_patient.ukt_date_death
    assert updated_patient.ukt_date_birth == incoming_patient.ukt_date_birth


def test_make_patient_match_row(incoming_patient, existing_patient):
    match_row = _incoming_patient_test(incoming_patient, existing_patient, "foo")
    assert match_row["DB RR_No"] == existing_patient.rr_no
    assert match_row["DB Surname"] == existing_patient.surname
    assert match_row["DB Forename"] == existing_patient.forename
    assert match_row["DB Sex"] == existing_patient.sex
    assert match_row["DB Date Birth"] == existing_patient.ukt_date_birth
    assert match_row["DB NHS Number"] == existing_patient.new_nhs_no
    match_row = _incoming_patient_test(incoming_patient, None, "bar")
    assert len(match_row) == 8


def _incoming_patient_test(incoming_patient, existing_patient, match_type):
    result = make_patient_match_row(incoming_patient, existing_patient, match_type)
    assert result["Match Type"] == match_type
    assert result["UKTSSA_No"] == incoming_patient.uktssa_no
    assert result["File RR_No"] is None
    assert result["File Surname"] == incoming_patient.surname
    assert result["File Forename"] == incoming_patient.forename
    assert result["File Sex"] == incoming_patient.sex
    assert result["File Date Birth"] == incoming_patient.ukt_date_birth
    assert result["File NHS Number"] == incoming_patient.new_nhs_no
    return result
