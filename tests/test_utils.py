import logging
import os
import sys
from datetime import datetime
from io import StringIO
from unittest import mock

import pandas as pd
import pytest
from faker import Faker
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker
from ukrr_models.nhsbt_models import Base, UKT_Patient, UKT_Transplant

from nhsbt_import.utils import (
    add_df_row,
    args_parse,
    check_missing_patients,
    check_missing_transplants,
    create_df,
    create_incoming_patient,
    create_incoming_transplant,
    create_logs,
    create_session,
    make_patient_match_row,
    update_nhsbt_patient,
)

fake = Faker()


@pytest.fixture
def logger():
    # create a logger instance
    logger = logging.getLogger(__name__)

    # set the logger level
    logger.setLevel(logging.DEBUG)

    # create a console handler with a formatter
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # add the console handler to the logger
    logger.addHandler(console_handler)

    # return the logger
    return logger


@pytest.fixture()
def session():
    # Create an in-memory SQLite database for testing
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)

    # Create a new session for each test
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    # Rollback the transaction to clean up after the test
    session.rollback()
    session.close()


@pytest.fixture
def df_data():
    fake = Faker()
    return {"Name": fake.name(), "Age": fake.random_int(min=18, max=99)}


@pytest.fixture
def uktssa_data():
    return pd.Series([fake.unique.random_number(digits=6) for _ in range(10)])


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


def test_add_df_row(df_data):
    df = pd.DataFrame({"Name": ["Alice", "Bob"], "Age": [25, 30]})
    expected_df = pd.DataFrame(
        {"Name": ["Alice", "Bob", df_data["Name"]], "Age": [25, 30, df_data["Age"]]}
    )
    result_df = add_df_row(df, df_data)
    assert expected_df.equals(result_df)


def test_args_parse(mocker):
    # Test that everything works with correct input
    mock_arg = ["-d", fake.file_path(depth=1)]
    mocker.patch("os.path.exists", return_value=True)
    mocker.patch("os.path.isdir", return_value=True)
    args = args_parse(mock_arg)
    assert args.directory == mock_arg[1]

    # Test help text displayed when no input given
    captured = StringIO()
    sys.stderr = captured
    with pytest.raises(SystemExit):
        args_parse([""])
    assert "usage:" in captured.getvalue()

    # Test when path doesn't exist
    mocker.patch("os.path.exists", return_value=False)
    with pytest.raises(NotADirectoryError):
        args_parse(mock_arg)

    # Test when path is not a directory
    mocker.patch("os.path.isdir", return_value=False)
    with pytest.raises(NotADirectoryError):
        args_parse(mock_arg)


def test_check_missing_patients(session: Session):
    # Create some test data
    db_data = [12345, 67890, 54321]
    file_data = pd.Series([12345, 67890, 99999])

    # Add test data to the database
    for uktssa_no in db_data:
        session.add(UKT_Patient(uktssa_no=uktssa_no))
    session.commit()

    # Call the function
    missing_patients = check_missing_patients(session, file_data)

    # Check the result
    assert missing_patients == [54321]


def test_check_missing_transplants(session: Session):
    # Create some test data
    db_data = ["100_1", "100_2", "100_3"]
    file_data = pd.Series(["100_1", "100_2", "100_4"])

    # Add test data to the database
    for registration_id in db_data:
        session.add(UKT_Transplant(registration_id=registration_id))
    session.commit()

    # Call the function
    missing_transplants = check_missing_transplants(session, file_data)

    # Check the result
    assert missing_transplants == ["100_3"]


def test_create_df():
    name = "test"
    df_columns = {"test": fake.pylist()}
    df = create_df(name, df_columns)

    # Assert that the dataframe has the correct columns
    expected_columns = df_columns[name]
    assert list(df.columns) == expected_columns

    # Assert that the dataframe is empty
    assert df.empty


def test_create_incoming_patient_valid_input():
    fake_date = fake.date()
    row = {
        "UKTR_ID": fake.random_int(),
        "UKTR_RSURNAME": fake.last_name(),
        "UKTR_RFORENAME": fake.first_name(),
        "UKTR_RSEX": fake.random_element(elements=("M", "F")),
        "UKTR_RPOSTCODE": fake.postcode(),
        "UKTR_RNHS_NO": fake.random_number(digits=10),
        "UKTR_RCHI_NO_NI": fake.random_number(digits=8),
        "UKTR_RCHI_NO_SCOT": fake.random_number(digits=10),
        "UKTR_DDATE": fake_date,
        "UKTR_RDOB": fake_date,
    }
    index = fake.random_int(min=1, max=99999)
    log = fake.pystr()

    patient = create_incoming_patient(index, row, log)
    assert patient.uktssa_no == row["UKTR_ID"]
    assert patient.surname == row["UKTR_RSURNAME"]
    assert patient.forename == row["UKTR_RFORENAME"]
    assert patient.sex == row["UKTR_RSEX"]
    assert patient.post_code == row["UKTR_RPOSTCODE"]
    assert patient.new_nhs_no == row["UKTR_RNHS_NO"]
    assert patient.chi_no == row["UKTR_RCHI_NO_SCOT"]
    assert patient.hsc_no == row["UKTR_RCHI_NO_NI"]
    assert patient.rr_no is None
    assert patient.ukt_date_death == datetime.strptime(fake_date, "%Y-%m-%d")
    assert patient.ukt_date_birth == datetime.strptime(fake_date, "%Y-%m-%d")


def test_create_incoming_patient_invalid_input(logger):
    fake_date = fake.date()
    row = {
        "UKTR_ID": fake.pystr(),
        "UKTR_RSURNAME": fake.last_name(),
        "UKTR_RFORENAME": fake.first_name(),
        "UKTR_RSEX": fake.random_element(elements=("M", "F")),
        "UKTR_RPOSTCODE": fake.postcode(),
        "UKTR_RNHS_NO": fake.random_number(digits=10),
        "UKTR_RCHI_NO_NI": fake.random_number(digits=8),
        "UKTR_RCHI_NO_SCOT": fake.random_number(digits=10),
        "UKTR_DDATE": datetime.strptime(fake_date, "%Y-%m-%d"),
        "UKTR_RDOB": datetime.strptime(fake_date, "%Y-%m-%d"),
    }
    index = fake.random_int(min=1, max=6)

    # Call the function and check that it raises a ValueError with the expected message
    with pytest.raises(ValueError) as e:
        create_incoming_patient(index, row, logger)
    assert str(e.value) == f"UKTR_ID must be a valid number, check row {index}"


def test_create_incoming_transplant():
    # create a sample input row and transplant counter
    row = pd.Series(
        {
            "uktr_tx_id1": fake.random_number(digits=4),
            "uktr_txdate1": fake.date(),
            "uktr_dgrp1": fake.word(),
            "uktr_tx_type1": fake.word(),
            "uktr_tx_unit1": fake.company(),
            "uktr_faildate1": None,
            "UKTR_ID": fake.random_number(digits=4),
            "uktr_date_on1": fake.date(),
            "uktr_list_status1": fake.word(),
            "uktr_removal_date1": None,
            "uktr_endstat1": None,
            "uktr_tx_list1": fake.word(),
            "uktr_dial_at_tx1": fake.word(),
            "uktr_relationship1": fake.word(),
            "uktr_dsex1": fake.random_element(elements=("M", "F")),
            "uktr_cof1": fake.word(),
            "uktr_other_cof_text1": None,
            "uktr_cit_mins1": fake.random_number(digits=2),
            "uktr_hla_mm1": fake.random_number(digits=1),
            "uktr_suspension_1": None,
        }
    )
    transplant_counter = 1

    # call the create_incoming_transplant function
    result = create_incoming_transplant(row, transplant_counter)

    # assert that the result is an instance of UKT_Transplant
    assert isinstance(result, UKT_Transplant)

    # assert that the attributes of the UKT_Transplant instance are correct
    assert result.transplant_id == row[f"uktr_tx_id{transplant_counter}"]
    assert result.uktssa_no == row["UKTR_ID"]
    assert result.transplant_date == row[f"uktr_txdate{transplant_counter}"]
    assert result.transplant_type == row[f"uktr_dgrp{transplant_counter}"]
    assert result.transplant_organ == row[f"uktr_tx_type{transplant_counter}"]
    assert result.transplant_unit == row[f"uktr_tx_unit{transplant_counter}"]
    assert result.ukt_fail_date is None
    assert result.registration_id == f'{int(row["UKTR_ID"])}_{transplant_counter}'
    assert result.registration_date == row[f"uktr_date_on{transplant_counter}"]
    assert result.registration_date_type == row[f"uktr_list_status{transplant_counter}"]
    assert result.registration_end_date is None
    assert result.registration_end_status is None
    assert result.transplant_consideration == row[f"uktr_tx_list{transplant_counter}"]
    assert result.transplant_dialysis == row[f"uktr_dial_at_tx{transplant_counter}"]
    assert (
        result.transplant_relationship == row[f"uktr_relationship{transplant_counter}"]
    )
    assert result.transplant_sex == row[f"uktr_dsex{transplant_counter}"]
    assert result.cause_of_failure == row[f"uktr_cof{transplant_counter}"]
    assert result.cause_of_failure_text is None
    assert result.cit_mins == row[f"uktr_cit_mins{transplant_counter}"]
    assert result.hla_mismatch == row[f"uktr_hla_mm{transplant_counter}"]
    assert result.ukt_suspension is None


def test_create_logs():
    temp_dir = "./temp_logs"

    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)

    os.makedirs(temp_dir)

    with mock.patch("logging.config.fileConfig"):
        result = create_logs(temp_dir)

        assert isinstance(result, logging.Logger)

        assert result.name == "nhsbt_import"

    # Remove the temporary directory
    os.rmdir(temp_dir)


# def test_create_logs(mocker):
#     # Create a mock logging config dictionary
#     logconf = {
#         "version": 1,
#         "disable_existing_loggers": False,
#         "handlers": {
#             "console": {
#                 "class": "logging.StreamHandler",
#                 "level": "DEBUG",
#                 "formatter": "simple",
#             }
#         },
#         "loggers": {
#             "nhsbt_import": {
#                 "handlers": ["console"],
#                 "level": "DEBUG",
#             }
#         },
#         "formatters": {
#             "simple": {
#                 "format": "%(levelname)s %(message)s",
#             }
#         },
#     }

#     # Mock the yaml module to return the logconf dictionary
#     mocker.patch("yaml.safe_load", MagicMock(return_value=logconf))

#     # Call the create_logs function
#     logger = create_logs()

#     # Check that the logger has the correct attributes
#     assert isinstance(logger, logging.Logger)
#     assert logger.name == "nhsbt_import"
#     assert logger.level == logging.DEBUG
#     assert len(logger.handlers) == 1
#     assert isinstance(logger.handlers[0], logging.StreamHandler)
#     assert logger.handlers[0].level == logging.DEBUG
#     assert isinstance(logger.handlers[0].formatter, logging.Formatter)
#     assert logger.handlers[0].formatter._fmt == "%(levelname)s %(message)s"


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
