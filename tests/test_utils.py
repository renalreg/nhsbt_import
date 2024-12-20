import logging
import os
import sys
import datetime
from io import StringIO

import pandas as pd
import pytest
from faker import Faker
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker
from ukrr_models import nhsbt_models, rr_models  # type: ignore [import]

from nhsbt_import import utils

fake = Faker()


@pytest.fixture
def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


@pytest.fixture()
def nhsbt_session():
    engine = create_engine("sqlite:///:memory:")
    nhsbt_models.Base.metadata.create_all(bind=engine)

    # Create a new session for each test
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    # Rollback the transaction to clean up after the test
    session.rollback()
    session.close()


@pytest.fixture()
def rr_session():
    engine = create_engine("sqlite:///:memory:")
    rr_models.Base.metadata.create_all(bind=engine)

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
    return {"Name": fake.name(), "Age": fake.random_int(min=18, max=99)}


@pytest.fixture
def uktssa_data():
    return pd.Series([fake.unique.random_number(digits=6) for _ in range(10)])


@pytest.fixture
def incoming_patient():
    return nhsbt_models.UKTPatient(
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
        ukt_date_birth=fake.date(pattern="%d-%m-%Y"),
    )


@pytest.fixture
def existing_patient():
    return nhsbt_models.UKTPatient(
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
        ukt_date_birth=fake.date_time(),
    )


@pytest.fixture
def incoming_transplant():
    return nhsbt_models.UKTTransplant(
        uktssa_no=fake.random_number(digits=6),
        transplant_id=fake.uuid4(),
        registration_id=fake.random_number(digits=8),
        transplant_date=fake.date(pattern="%d-%m-%Y"),
        transplant_type=fake.word(),
        transplant_organ=fake.word(),
        transplant_unit=fake.word(),
        registration_date=fake.date(pattern="%d-%m-%Y"),
        registration_date_type=fake.word(),
        registration_end_date=fake.date(pattern="%d-%m-%Y"),
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
    return nhsbt_models.UKTTransplant(
        transplant_id=fake.uuid4(),
        registration_id=fake.random_number(digits=8),
        transplant_date=fake.date_time(),
        transplant_type=fake.word(),
        transplant_organ=fake.word(),
        transplant_unit=fake.word(),
        registration_date=fake.date_time(),
        registration_date_type=fake.word(),
        registration_end_date=fake.date_time(),
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
    result_df = utils.add_df_row(df, df_data)
    assert expected_df.equals(result_df)


def test_args_parse(mocker):
    mock_arg = ["-d", fake.file_path(depth=1)]
    mocker.patch("os.path.exists", return_value=True)
    mocker.patch("os.path.isdir", return_value=True)
    args = utils.args_parse(mock_arg)
    assert args.directory == mock_arg[1]

    captured = StringIO()
    sys.stderr = captured
    with pytest.raises(SystemExit):
        utils.args_parse([""])
    assert "usage:" in captured.getvalue()

    mocker.patch("os.path.exists", return_value=False)
    with pytest.raises(NotADirectoryError):
        utils.args_parse(mock_arg)

    mocker.patch("os.path.isdir", return_value=False)
    with pytest.raises(NotADirectoryError):
        utils.args_parse(mock_arg)


def test_check_missing_patients(nhsbt_session: Session):
    db_data = [12345, 67890, 54321]
    file_data = [12345, 67890, 99999]

    for uktssa_no in db_data:
        nhsbt_session.add(nhsbt_models.UKTPatient(uktssa_no=uktssa_no))
    nhsbt_session.commit()

    missing_patients = utils.check_missing_patients(nhsbt_session, file_data)

    assert missing_patients == [54321]


def test_check_missing_transplants(nhsbt_session: Session):
    db_data = ["100_1", "100_2", "100_3"]
    file_data = ["100_1", "100_2", "100_4"]

    for registration_id in db_data:
        nhsbt_session.add(nhsbt_models.UKTTransplant(registration_id=registration_id))
    nhsbt_session.commit()

    missing_transplants = utils.check_missing_transplants(nhsbt_session, file_data)

    assert missing_transplants == ["100_3"]


def test_compare_patients(incoming_patient, existing_patient):
    result = utils.compare_patients(incoming_patient, incoming_patient)
    assert result is True

    result = utils.compare_patients(incoming_patient, existing_patient)
    assert result is False


def test_compare_transplants(incoming_transplant, existing_transplant):
    result = utils.compare_transplants(incoming_transplant, incoming_transplant)
    assert result is True

    result = utils.compare_transplants(incoming_transplant, existing_transplant)
    assert result is False


def test_create_df():
    name = fake.word()
    df_columns = {name: fake.pylist()}
    df = utils.create_df(name, df_columns)

    expected_columns = df_columns[name]
    assert list(df.columns) == expected_columns

    assert df.empty


def test_create_incoming_patient_valid_input():
    fake_date = fake.date()
    row = {
        "UKTR_ID": fake.random_int(),
        "UKTR_RSURNAME": fake.last_name(),
        "UKTR_RFORENAME": fake.first_name(),
        "UKTR_RSEX": fake.random_element(elements=("1", "2")),
        "UKTR_RPOSTCODE": fake.pystr_format(string_format="??## #??").upper(),
        "UKTR_RNHS_NO": fake.random_number(digits=10),
        "UKTR_RCHI_NO_NI": fake.random_number(digits=8),
        "UKTR_RCHI_NO_SCOT": fake.random_number(digits=10),
        "UKTR_DDATE": fake_date,
        "UKTR_RDOB": fake_date,
    }

    index = fake.random_int(min=1, max=99999)
    patient = utils.create_incoming_patient(index, row)

    assert isinstance(patient.uktssa_no, (int, type(None)))
    assert isinstance(patient.surname, (str, type(None)))
    assert isinstance(patient.forename, (str, type(None)))
    assert patient.sex in ("0", "1", "2", "9")
    assert isinstance(patient.post_code, (str, type(None)))
    assert isinstance(patient.new_nhs_no, (int, type(None)))
    assert isinstance(patient.chi_no, (int, type(None)))
    assert patient.hsc_no == row["UKTR_RCHI_NO_NI"]
    assert patient.rr_no is None
    assert isinstance(patient.ukt_date_death, (datetime.date, type(None)))
    assert isinstance(patient.ukt_date_birth, (datetime.date, type(None)))


def test_create_incoming_patient_invalid_uktr_id():
    row = {
        "UKTR_ID": fake.pystr(),
        "UKTR_RSURNAME": fake.last_name(),
        "UKTR_RFORENAME": fake.first_name(),
        "UKTR_RSEX": fake.random_element(elements=("M", "F")),
        "UKTR_RPOSTCODE": fake.postcode(),
        "UKTR_RNHS_NO": fake.random_number(digits=10),
        "UKTR_RCHI_NO_NI": fake.random_number(digits=8),
        "UKTR_RCHI_NO_SCOT": fake.random_number(digits=10),
        "UKTR_DDATE": fake.date,
        "UKTR_RDOB": fake.date,
    }
    index = fake.random_int(min=1, max=6)

    with pytest.raises(ValueError) as e:
        utils.create_incoming_patient(index, row)
    assert str(e.value) == f"UKTR_ID must be a valid number, check row {index + 1}"


def test_create_incoming_transplant():
    # This test fails sometimes, I suspect it's to do with the changes
    # to boolean for the uktr_suspension I saw in a previous commit
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
            "uktr_dsex1": fake.random_element(elements=("M", "F", None)),
            "uktr_cof1": fake.word(),
            "uktr_other_cof_text1": None,
            "uktr_cit_mins1": str(fake.random_number(digits=2)),
            "uktr_hla_mm1": str(fake.random_number(digits=1)),
            "uktr_suspension_1": fake.random_element(elements=("1", "0", None)),
        }
    )
    transplant_counter = 1
    index = 1

    result = utils.create_incoming_transplant(index, row, transplant_counter)

    assert isinstance(result, nhsbt_models.UKTTransplant)
    assert isinstance(result.registration_id, (str, type(None)))
    assert isinstance(result.uktssa_no, (int, type(None)))
    assert isinstance(result.transplant_id, (int, type(None)))
    assert isinstance(result.transplant_type, (str, type(None)))
    assert isinstance(result.transplant_organ, (str, type(None)))
    assert isinstance(result.transplant_unit, (str, type(None)))
    assert isinstance(result.rr_no, (int, type(None)))
    assert isinstance(result.transplant_date, (datetime.datetime, type(None)))
    assert isinstance(result.ukt_fail_date, (datetime.datetime, type(None)))
    assert isinstance(result.registration_date, (datetime.datetime, type(None)))
    assert isinstance(result.registration_date_type, (str, type(None)))
    assert isinstance(result.registration_end_date, (datetime.datetime, type(None)))
    assert isinstance(result.registration_end_status, (str, type(None)))
    assert isinstance(result.transplant_consideration, (str, type(None)))
    assert isinstance(result.transplant_dialysis, (str, type(None)))
    assert isinstance(result.transplant_relationship, (str, type(None)))
    assert result.transplant_sex in ("0", "1", "2", "9", None)
    assert isinstance(result.cause_of_failure, (str, type(None)))
    assert isinstance(result.cause_of_failure_text, (str, type(None)))
    assert isinstance(result.cit_mins, (str, type(None)))
    assert isinstance(result.hla_mismatch, (str, type(None)))
    assert result.ukt_suspension in (True, False, None)


def test_create_logs(mocker):
    temp_dir = "./temp_logs"

    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)

    os.makedirs(temp_dir)

    with mocker.patch("logging.config.fileConfig"):
        result = utils.create_logs(temp_dir)

        assert isinstance(result, logging.Logger)
        assert result.name == "nhsbt_import"

    # Remove the temporary directory
    os.rmdir(temp_dir)


def test_create_output_dfs(df_columns):
    output_dfs = utils.create_output_dfs(df_columns)

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
    session = utils.create_session()

    assert isinstance(session, Session)
    assert isinstance(session.bind, Engine)
    assert session.bind.driver == "pyodbc"
    assert session.bind.url.host == "rr-sql-live"
    assert session.bind.url.database == "renalreg"


def test_deleted_patient_check(rr_session):
    mock_results = [(1,), (3,), (5,)]
    for mock_result in mock_results:
        rr_session.add(rr_models.UKRR_Deleted_Patient(uktssa_no=mock_result[0]))
    rr_session.commit()

    file_patients = [1, 2, 3, 4, 5]

    deleted_patients = utils.deleted_patient_check(rr_session, file_patients)

    # Check the results
    assert set(deleted_patients) == {1, 3, 5}

    # # Ensure that the session's query method was called
    # session.query.assert_called_once_with(UKRR_Deleted_Patient.uktssa_no)
    # session.query.return_value.all.assert_called_once_with()


def test_format_bool():
    true_test_values = ["1", "1.0", 1, 1.0, "True", "true", True]

    for value in true_test_values:
        result = utils.format_bool(value)
        assert result is True

    false_test_values = ["0", "0.0", 0, 0.0, "False", "false", False]

    for value in false_test_values:
        result = utils.format_bool(value)
        assert result is False

    none_test_values = ["2", "abc", None, "unknown", "", " ", 42]

    for value in none_test_values:
        result = utils.format_bool(value)
        assert result is None


def test_format_date():
    result_date_year_first = utils.format_date("2023-11-22")
    assert result_date_year_first == datetime.datetime.strptime(
        "2023-11-22", "%Y-%m-%d"
    )

    result_date_year_first = utils.format_date("1976-08-09")
    assert result_date_year_first == datetime.datetime.strptime(
        "1976-08-09", "%Y-%m-%d"
    )

    result_date_using_slash = utils.format_date("2022/03/15")
    assert result_date_using_slash == datetime.datetime.strptime(
        "2022-03-15", "%Y-%m-%d"
    )

    result_date_day_first = utils.format_date("15-06-1995")
    assert result_date_day_first == datetime.datetime.strptime("1995-06-15", "%Y-%m-%d")

    result_date_day_first = utils.format_date("04/06/1995")
    assert result_date_day_first == datetime.datetime.strptime("1995-06-04", "%Y-%m-%d")

    result_date_day_first = utils.format_date("06-04-1995")
    assert result_date_day_first == datetime.datetime.strptime("1995-04-06", "%Y-%m-%d")

    result_date_not_a_date = utils.format_date("2000-20-20")
    assert result_date_not_a_date is None

    result_empty_string = utils.format_date("")
    assert result_empty_string is None

    result_none = utils.format_date(None)
    assert result_none is None

    result_invalid_string = utils.format_date("invalid_date")
    assert result_invalid_string is None

    result_datetime_object = utils.format_date(datetime.datetime(1995, 5, 7, 0, 0, 0))
    assert result_datetime_object == datetime.datetime(1995, 5, 7, 0, 0, 0)

    result_datetime_in_date_out = utils.format_date(
        datetime.datetime(1995, 5, 7, 0, 0, 0), True
    )
    assert result_datetime_in_date_out == datetime.date(1995, 5, 7)

    result_date_object = utils.format_date(datetime.date(1995, 5, 7))
    assert result_date_object == datetime.datetime(1995, 5, 7, 0, 0, 0)

    result_date_in_date_out = utils.format_date(datetime.date(1995, 5, 7), True)
    assert result_date_in_date_out == datetime.date(1995, 5, 7)


def test_format_int():
    valid_values = [42, "42", 3.14, "1000", "0", "123"]

    for value in valid_values:
        result = utils.format_int(value)
        assert result == int(value)

    result_nan = utils.format_int(pd.NA)
    assert result_nan is None

    result_none = utils.format_int(None)
    assert result_none is None

    result_invalid_value = utils.format_int("abc")
    assert result_invalid_value is None


def test_format_str():
    valid_values = ["Hello", 42, 3.14, "123", pd.NA, {"key": "value"}]

    for value in valid_values:
        result = utils.format_str(value)
        if pd.isna(value):
            assert result is None
        else:
            assert result == str(value)

    assert utils.format_str(None) is None


def test_format_postcode():
    result = utils.format_postcode(123)
    assert result == "123"

    result = utils.format_postcode("abcd345efghi")
    assert result == "ABCD345EFGHI"

    result = utils.format_postcode("b")
    assert result == "B"

    result = utils.format_postcode("ab 12 3 cd")
    assert result == "AB12 3CD"

    result_valid = utils.format_postcode("AB12  3CD")
    assert result_valid == "AB12 3CD"

    result_valid = utils.format_postcode("ab12 3cd")
    assert result_valid == "AB12 3CD"

    result_valid_no_spaces = utils.format_postcode("ab123cd")
    assert result_valid_no_spaces == "AB12 3CD"

    result_valid_no_spaces = utils.format_postcode("ab1")
    assert result_valid_no_spaces == "AB1"

    result_empty = utils.format_postcode("")
    assert result_empty == ""

    result_empty = utils.format_postcode(" ")
    assert result_empty == ""

    result_valid_no_spaces = utils.format_postcode("ab13cd")
    assert result_valid_no_spaces == "AB1 3CD"

    result_none = utils.format_postcode(None)
    assert result_none is None

    result_invalid_short = utils.format_postcode("abc")
    assert result_invalid_short == "ABC"


def test_get_input_file_path_single_csv(mocker):
    with mocker.patch("os.listdir", return_value=["file.csv"]):
        directory = "/path/to/directory"
        result = utils.get_input_file_path(directory)

    expected_path = os.path.join(directory, "file.csv")

    assert result == expected_path


def test_get_input_file_path_multiple_csv(mocker):
    with mocker.patch("os.listdir", return_value=["file1.csv", "file2.csv"]):
        directory = "/path/to/directory"

        with pytest.raises(ValueError) as excinfo:
            utils.get_input_file_path(directory)

        assert (
            str(excinfo.value)
            == f"Expected to find one import CSV file in {directory}, but found 2 files."
        )


def test_make_patient_match_row(incoming_patient, existing_patient):
    match_row = _incoming_patient_test("foo", incoming_patient, existing_patient)
    assert match_row["RR_No"] == existing_patient.rr_no
    assert match_row["Surname - RR"] == existing_patient.surname
    assert match_row["Forename - RR"] == existing_patient.forename
    assert match_row["Sex - RR"] == existing_patient.sex
    assert match_row["Date Birth - RR"] == existing_patient.ukt_date_birth.date()
    assert isinstance(match_row["Date Death - RR"], (datetime.date, type(None)))
    assert match_row["NHS Number - RR"] == existing_patient.new_nhs_no
    assert match_row["CHI Number - RR"] == existing_patient.chi_no
    assert match_row["HSC Number - RR"] == existing_patient.hsc_no
    assert match_row["Postcode - RR"] == existing_patient.post_code
    match_row = _incoming_patient_test("bar", incoming_patient, None)
    assert len(match_row) == 11


def _incoming_patient_test(match_type, incoming_patient, existing_patient):
    result = utils.make_patient_match_row(
        match_type, incoming_patient, existing_patient
    )
    assert result["Match Type"] == match_type
    assert result["UKTSSA_No"] == incoming_patient.uktssa_no
    assert result["Surname - NHSBT"] == incoming_patient.surname
    assert result["Forename - NHSBT"] == incoming_patient.forename
    assert result["Sex - NHSBT"] == incoming_patient.sex
    assert isinstance(result["Date Birth - NHSBT"], (datetime.date, type(None)))
    assert isinstance(result["Date Death - NHSBT"], (datetime.date, type(None)))
    assert result["NHS Number - NHSBT"] == incoming_patient.new_nhs_no
    assert result["CHI Number - NHSBT"] == incoming_patient.chi_no
    assert result["HSC Number - NHSBT"] == incoming_patient.hsc_no
    assert result["Postcode - NHSBT"] == incoming_patient.post_code
    return result


def test_make_transplant_match_row(incoming_transplant, existing_transplant):
    match_type = fake.word()
    row = utils.make_transplant_match_row(
        match_type, incoming_transplant, existing_transplant
    )
    assert isinstance(row, dict)
    assert row.get("Match Type") == match_type
    assert row.get("UKTSSA_No") == incoming_transplant.uktssa_no
    assert row.get("Transplant ID - NHSBT") == incoming_transplant.transplant_id
    assert isinstance(row.get("Transplant Date - NHSBT"), (datetime.date, type(None)))
    assert row.get("Transplant Type - NHSBT") == incoming_transplant.transplant_type
    assert row.get("Transplant Organ - NHSBT") == incoming_transplant.transplant_organ
    assert row.get("Transplant Unit - NHSBT") == incoming_transplant.transplant_unit
    assert isinstance(row.get("Registration Date - NHSBT"), (datetime.date, type(None)))
    assert (
        row.get("Registration Date Type - NHSBT")
        == incoming_transplant.registration_date_type
    )
    assert isinstance(
        row.get("Registration End Date - NHSBT"), (datetime.date, type(None))
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
    utils.update_nhsbt_patient(incoming_patient, existing_patient)

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
    utils.update_nhsbt_transplant(incoming_transplant, existing_transplant)
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
