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
from ukrr_models.nhsbt_models import Base, UKTPatient, UKTTransplant  # type: ignore [import]

from nhsbt_import.utils import (
    add_df_row,
    args_parse,
    check_missing_patients,
    check_missing_transplants,
    create_df,
    create_incoming_patient,
    create_incoming_transplant,
    create_logs,
    create_output_dfs,
    create_session,
    make_patient_match_row,
    make_transplant_match_row,
    update_nhsbt_patient,
    update_nhsbt_transplant,
)

fake = Faker()


@pytest.fixture
def convert_to_date():
    def _convert_to_date(date):
        return datetime.strptime(date, "%Y-%m-%d").date()

    return _convert_to_date


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
def df_columns():
    columns = {
        "new_transplants": [fake.word() for _ in range(5)],
        "updated_transplants": [fake.word() for _ in range(6)],
        "other_sheet": [fake.word() for _ in range(4)],
    }
    columns["new_transplants"].append("UKT Suspension - NHSBT")
    columns["updated_transplants"].append("UKT Suspension - NHSBT")
    columns["updated_transplants"].append("UKT Suspension - RR")
    return columns


@pytest.fixture
def df_data():
    fake = Faker()
    return {"Name": fake.name(), "Age": fake.random_int(min=18, max=99)}


@pytest.fixture
def uktssa_data():
    return pd.Series([fake.unique.random_number(digits=6) for _ in range(10)])


@pytest.fixture
def incoming_patient():
    return UKTPatient(
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
    return UKTPatient(
        uktssa_no=fake.random_number(digits=6),
        surname=fake.last_name(),
        forename=fake.first_name(),
        sex=fake.random_element(elements=("M", "F")),
        post_code=fake.postcode(),
        new_nhs_no=fake.random_number(digits=10),
        chi_no=None,
        hsc_no=None,
        rr_no=fake.random_number(digits=6),
        ukt_date_death=None,
        ukt_date_birth=None,
    )


@pytest.fixture
def incoming_transplant():
    return UKTTransplant(
        uktssa_no=fake.random_number(digits=6),
        transplant_id=fake.uuid4(),
        registration_id=fake.random_number(digits=8),
        transplant_date=fake.date(),
        transplant_type=fake.word(),
        transplant_organ=fake.word(),
        transplant_unit=fake.word(),
        registration_date=fake.date(),
        registration_date_type=fake.word(),
        registration_end_date=fake.date(),
        registration_end_status=fake.word(),
        transplant_consideration=fake.word(),
        transplant_dialysis=fake.word(),
        transplant_relationship=fake.word(),
        transplant_sex=fake.word(),
        cause_of_failure=fake.word(),
        cause_of_failure_text=fake.word(),
        cit_mins=fake.random_number(digits=3),
        hla_mismatch=fake.word(),
        ukt_suspension=fake.word(),
    )


@pytest.fixture
def existing_transplant():
    return UKTTransplant(
        transplant_id=fake.uuid4(),
        registration_id=fake.random_number(digits=8),
        transplant_date=fake.date(),
        transplant_type=fake.word(),
        transplant_organ=fake.word(),
        transplant_unit=fake.word(),
        registration_date=fake.date(),
        registration_date_type=fake.word(),
        registration_end_date=fake.date(),
        registration_end_status=fake.word(),
        transplant_consideration=fake.word(),
        transplant_dialysis=fake.word(),
        transplant_relationship=fake.word(),
        transplant_sex=fake.word(),
        cause_of_failure=fake.word(),
        cause_of_failure_text=fake.word(),
        cit_mins=fake.random_number(digits=3),
        hla_mismatch=fake.word(),
        ukt_suspension=fake.word(),
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
    file_data = [12345, 67890, 99999]

    # Add test data to the database
    for uktssa_no in db_data:
        session.add(UKTPatient(uktssa_no=uktssa_no))
    session.commit()

    # Call the function
    missing_patients = check_missing_patients(session, file_data)

    # Check the result
    assert missing_patients == [54321]


def test_check_missing_transplants(session: Session):
    # Create some test data
    db_data = ["100_1", "100_2", "100_3"]
    file_data = ["100_1", "100_2", "100_4"]

    # Add test data to the database
    for registration_id in db_data:
        session.add(UKTTransplant(registration_id=registration_id))
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


def test_create_incoming_patient_valid_input(convert_to_date):
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
    assert patient.ukt_date_death == convert_to_date(fake_date)
    assert patient.ukt_date_birth == convert_to_date(fake_date)


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


def test_create_incoming_transplant(convert_to_date):
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
            "uktr_cit_mins1": str(fake.random_number(digits=2)),
            "uktr_hla_mm1": str(fake.random_number(digits=1)),
            "uktr_suspension_1": None,
        }
    )
    transplant_counter = 1

    # call the create_incoming_transplant function
    result = create_incoming_transplant(row, transplant_counter)

    # assert that the result is an instance of UKTTransplant
    assert isinstance(result, UKTTransplant)

    # assert that the attributes of the UKTTransplant instance are correct
    assert result.transplant_id == row[f"uktr_tx_id{transplant_counter}"]
    assert result.uktssa_no == row["UKTR_ID"]
    assert result.transplant_date == convert_to_date(
        row[f"uktr_txdate{transplant_counter}"]
    )
    assert result.transplant_type == row[f"uktr_dgrp{transplant_counter}"]
    assert result.transplant_organ == row[f"uktr_tx_type{transplant_counter}"]
    assert result.transplant_unit == row[f"uktr_tx_unit{transplant_counter}"]
    assert result.ukt_fail_date is None
    assert result.registration_id == f'{int(row["UKTR_ID"])}_{transplant_counter}'
    assert result.registration_date == convert_to_date(
        row[f"uktr_date_on{transplant_counter}"]
    )

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


def test_create_output_dfs(df_columns):
    output_dfs = create_output_dfs(df_columns)

    assert isinstance(output_dfs, dict)
    assert set(output_dfs.keys()) == set(df_columns.keys())

    for sheet, cols in df_columns.items():
        assert isinstance(output_dfs[sheet], pd.DataFrame)
        assert output_dfs[sheet].shape == (0, len(cols))

        if "UKT Suspension - NHSBT" in cols:
            assert output_dfs[sheet]["UKT Suspension - NHSBT"].dtype == bool

        if "UKT Suspension - RR" in cols:
            assert output_dfs[sheet]["UKT Suspension - RR"].dtype == bool


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


def test_make_patient_match_row(incoming_patient, existing_patient):
    match_row = _incoming_patient_test("foo", incoming_patient, existing_patient)
    assert match_row["RR_No"] == existing_patient.rr_no
    assert match_row["Surname - RR"] == existing_patient.surname
    assert match_row["Forename - RR"] == existing_patient.forename
    assert match_row["Sex - RR"] == existing_patient.sex
    assert match_row["Date Birth - RR"] == existing_patient.ukt_date_birth
    assert match_row["NHS Number - RR"] == existing_patient.new_nhs_no
    match_row = _incoming_patient_test("bar", incoming_patient, None)
    assert len(match_row) == 7


def _incoming_patient_test(match_type, incoming_patient, existing_patient):
    result = make_patient_match_row(match_type, incoming_patient, existing_patient)
    assert result["Match Type"] == match_type
    assert result["UKTSSA_No"] == incoming_patient.uktssa_no
    assert result["Surname - NHSBT"] == incoming_patient.surname
    assert result["Forename - NHSBT"] == incoming_patient.forename
    assert result["Sex - NHSBT"] == incoming_patient.sex
    assert result["Date Birth - NHSBT"] == incoming_patient.ukt_date_birth
    assert result["NHS Number - NHSBT"] == incoming_patient.new_nhs_no
    return result


def test_make_transplant_match_row(incoming_transplant, existing_transplant):
    match_type = fake.word()
    row = make_transplant_match_row(
        match_type, incoming_transplant, existing_transplant
    )
    assert isinstance(row, dict)
    assert row.get("Match Type") == match_type
    assert row.get("UKTSSA_No") == incoming_transplant.uktssa_no
    assert row.get("Transplant ID - NHSBT") == incoming_transplant.transplant_id
    assert row.get("Registration ID - NHSBT") == incoming_transplant.registration_id

    assert row.get("Transplant Date - NHSBT") == incoming_transplant.transplant_date
    assert row.get("Transplant Type - NHSBT") == incoming_transplant.transplant_type
    assert row.get("Transplant Organ - NHSBT") == incoming_transplant.transplant_organ
    assert row.get("Transplant Unit - NHSBT") == incoming_transplant.transplant_unit
    assert row.get("Registration Date - NHSBT") == incoming_transplant.registration_date
    assert (
        row.get("Registration Date Type - NHSBT")
        == incoming_transplant.registration_date_type
    )
    assert (
        row.get("Registration End Date - NHSBT")
        == incoming_transplant.registration_end_date
    )
    assert (
        row.get("Registration End Status - NHSBT")
        == incoming_transplant.registration_end_status
    )
    assert (
        row.get("Transplant Consideration - NHSBT")
        == incoming_transplant.transplant_consideration
    )
    assert (
        row.get("Transplant Dialysis - NHSBT")
        == incoming_transplant.transplant_dialysis
    )
    assert (
        row.get("Transplant Relationship - NHSBT")
        == incoming_transplant.transplant_relationship
    )


def test_update_nhsbt_patient(incoming_patient, existing_patient):
    # Call the update_nhsbt_patient function to update the existing patient
    update_nhsbt_patient(incoming_patient, existing_patient)

    # Check that the existing patient has been updated correctly
    assert existing_patient.uktssa_no == incoming_patient.uktssa_no
    assert existing_patient.surname == incoming_patient.surname
    assert existing_patient.forename == incoming_patient.forename
    assert existing_patient.sex == incoming_patient.sex
    assert existing_patient.post_code == incoming_patient.post_code
    assert existing_patient.new_nhs_no == incoming_patient.new_nhs_no
    assert existing_patient.chi_no == incoming_patient.chi_no
    assert existing_patient.hsc_no == incoming_patient.hsc_no
    assert existing_patient.rr_no == existing_patient.rr_no
    assert existing_patient.ukt_date_death == incoming_patient.ukt_date_death
    assert existing_patient.ukt_date_birth == incoming_patient.ukt_date_birth


def test_update_nhsbt_transplant(existing_transplant, incoming_transplant):
    update_nhsbt_transplant(incoming_transplant, existing_transplant)
    assert existing_transplant.rr_no == existing_transplant.rr_no
    assert existing_transplant.uktssa_no == incoming_transplant.uktssa_no
    assert existing_transplant.transplant_id == incoming_transplant.transplant_id
    assert existing_transplant.registration_id == incoming_transplant.registration_id
    assert existing_transplant.transplant_date == incoming_transplant.transplant_date
    assert existing_transplant.transplant_type == incoming_transplant.transplant_type
    assert existing_transplant.transplant_organ == incoming_transplant.transplant_organ
    assert existing_transplant.transplant_unit == incoming_transplant.transplant_unit
    assert (
        existing_transplant.registration_date == incoming_transplant.registration_date
    )
    assert (
        existing_transplant.registration_date_type
        == incoming_transplant.registration_date_type
    )
    assert (
        existing_transplant.registration_end_date
        == incoming_transplant.registration_end_date
    )
    assert (
        existing_transplant.registration_end_status
        == incoming_transplant.registration_end_status
    )
    assert (
        existing_transplant.transplant_consideration
        == incoming_transplant.transplant_consideration
    )
    assert (
        existing_transplant.transplant_dialysis
        == incoming_transplant.transplant_dialysis
    )
    assert (
        existing_transplant.transplant_relationship
        == incoming_transplant.transplant_relationship
    )
    assert existing_transplant.transplant_sex == incoming_transplant.transplant_sex
    assert existing_transplant.cause_of_failure == incoming_transplant.cause_of_failure
    assert (
        existing_transplant.cause_of_failure_text
        == incoming_transplant.cause_of_failure_text
    )
    assert existing_transplant.cit_mins == incoming_transplant.cit_mins
    assert existing_transplant.hla_mismatch == incoming_transplant.hla_mismatch
    assert existing_transplant.ukt_suspension == incoming_transplant.ukt_suspension
