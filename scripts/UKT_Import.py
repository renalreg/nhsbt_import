from datetime import datetime
import argparse
import csv
import logging
import logging.config
import os
import sys
import yaml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ukrr_models.nhsbt_models import UKT_Patient, UKT_Transplant
from ukrr_models.rr_models import UKRR_Patient, UKRR_Deleted_Patient

from rr_reports import ExcelLib


def format_date(str_date: str):
    date_formats = [
        '%d%b%Y',
        '%d-%b-%y'
    ]
    formatted_date = None
    for date_format in date_formats:
        try:
            formatted_date = datetime.strptime(str_date, date_format).date()
        except Exception:
            pass
    if formatted_date is None:
        print(str_date)
        raise Exception
    return formatted_date


def run(csv_reader, error_file: str='UKT_Errors.xls'):
    log = logging.getLogger('ukt_import')
    driver = 'SQL+Server+Native+Client+11.0'
    Engine = create_engine(f"mssql+pyodbc://rr-sql-live/renalreg?driver={driver}")

    Cursor = Engine.connect()

    # TODO: Update the comments in this to make sure they match up with the new way of working.
    # TODO: Put something in this to ignore 9999xxxxx Paed patients.
    # Note that 20 (but only 20?) got into the database last time this was run. This is puzzling.

    # TODO: This is not handling all the recent fields comprehensively.

    SessionMaker = sessionmaker(bind=Engine)
    Session = SessionMaker()

    excel_error_wb = ExcelLib.ExcelWB()
    excel_error_wb.AddSheet(
        "Match Differences",
        (
            "UKTSSA_No",
            "Match Type",
            "File RR_No",
            "File Surname",
            "File Forename",
            "File Sex",
            "File Date Birth",
            "File NHS Number",
            "DB RR_No",
            "DB Surname",
            "DB Forename",
            "DB Sex",
            "DB Date Birth",
            "DB NHS NUmber"
        ),
        0
    )
    excel_error_wb.AddSheet(
        "Patient Field Differences",
        ("UKTSSA_No", "Field", "File Value", "Previous Import Value"),
        0)
    excel_error_wb.AddSheet(
        'Transplant Field Differences',
        ("UKTSSA_No", "Transplant_ID", "Field", "File Value", "Previous Import Value"),
        0)
    excel_error_wb.AddSheet("Invalid Postcodes", ("UKTSSA_No", "Message", "Value"), 0)
    excel_error_wb.AddSheet("Invalid NHS Numbers", ("UKTSSA_No", "Value"), 0)
    excel_error_wb.AddSheet("Missing Patients", ("UKTSSA_No", ), 0)
    excel_error_wb.AddSheet("Missing Transplants", ("Transplant_ID", ), 0)

    patient_list = []
    transplant_list = []

    for line_number, row in enumerate(csv_reader, start=2):
        log.info("on line {}".format(line_number))
        row = list(row)

        if uktssa_no in (0, '', None):
            uktssa_no = None
        else:
            uktssa_no = int(uktssa_no)
            patient_list.append(uktssa_no)

        rr_no = row[1].replace('/', '').strip()
        if rr_no in ('', 0):
            rr_no = None
        else:
            rr_no = int(rr_no)

        # Skip paed patients
        if str(rr_no)[:4] == "9999":
            continue
        # Skip Q100 new patients
        elif str(rr_no)[:4] == "8888":
            continue

        # NOTE: A lot of this still as it was when
        # NHSBT supplied demographics. As there is
        # the suggestion that this might resume
        # the code is intentionally left with
        # the same logic.
        # You could probably make this configurable.

        surname = None
        forename = None
        sex = None
        post_code = None
        nhs_no = None

        ukt_date_death = row[2]
        if ukt_date_death in ('', 0):
            ukt_date_death = None
        else:
            ukt_date_death = format_date(ukt_date_death)

        ukt_date_birth = None

        results = Session.query(UKT_Patient).filter_by(uktssa_no=uktssa_no).all()

        if len(results) == 1:
            log.info("UKT Patient {} found in database".format(uktssa_no))
            ukt_patient = results[0]
            log.info("Updating record")
            if surname != ukt_patient.surname:
                ukt_patient.surname = surname
            if forename != ukt_patient.forename:
                ukt_patient.forename = forename
            if sex != ukt_patient.sex:
                ukt_patient.sex = sex
            if post_code != ukt_patient.post_code:
                ukt_patient.post_code = post_code
            if nhs_no != ukt_patient.nhs_no:
                ukt_patient.nhs_no = nhs_no
            if ukt_date_death != ukt_patient.ukr_date_death:
                ukt_patent.ukt_date_death = ukt_date_death
            if ukt_date_birth != ukt_patuient.ukt_date_birth:
                ukt_patient.ukt_date_birth = ukt_date_birth

            match_type = None

            # RR_No here comes from the matched file
            if rr_no != ukt_patient.rr_no \
                    and (ukt_patient.rr_no is not None or rr_no is not None):
                match_type = "Match Difference"
            elif rr_no is not None and ukt_patient.rr_no is None:
                # This should never happen as we now can't load the unmatched records.
                match_type = "New Match"
            try:
                rr_patient = Session.query(UKRR_Patient).filter_by(rr_no=rr_no).all()[0]
            except Exception:
                try:
                    rr_patient = Session.query(UKRR_Deleted_Patient).filter_by(rr_no=rr_no).all()[0]
                    match_type = "Match to Deleted Patient"
                except Exception:
                    match_type = "Match to Patient not in Database"

            if match_type is not None:
                excel_error_wb.Sheets['Match Differences'].WriteRow(
                    (
                        uktssa_no,
                        match_type,
                        rr_no,
                        surname,
                        forename,
                        sex,
                        ukt_date_birth,
                        nhs_no,
                        rr_patient.rr_no,
                        rr_patient.surname,
                        rr_patient.forename,
                        rr_patient.sex,
                        rr_patient.date_birth,
                        rr_patient.nhs_no
                    )
                )
            # Update the RR_No
            if rr_no is not None and rr_no != ukt_patient.rr_no:
                ukt_patient.rr_no = rr_no
        elif len(results) == 0:
            log.info("Add patient")
            ukt_patient = UKT_Patient(
                uktssa_no=uktssa_no,
                surname=surname,
                forename=forename,
                sex=sex,
                post_code=post_code,
                nhs_no=nhs_no,
                rr_no=rr_no,
                ukt_date_death=ukt_date_death,
                ukt_date_birth=ukt_date_birth
            )
            Session.add(ukt_patient)
        else:
            log.error("{} in the database multiple times".format(uktssa_no))

        # Transplants
        # for x in (15, 26, 37, 48, 59, 70): - 2011 file -
        # Note this was somewhat incorrect as this was the position of the TXID fields
        # whereas the full Transplant record started a couple of fields earlier.
        # In 2012 an extra field was added.
        # for i, x in enumerate((10, 24, 38, 52, 66, 80)):
        # Loss of PID for 2013
        # for i, x in enumerate((3, 17, 31, 45, 59, 73)):
        # More fields added in October 2016
        for i, x in enumerate((3, 22, 41, 60, 79, 98)):

            registration_id = str(uktssa_no) + "_" + str(i + 1)

            registration_date = row[x]  # 1
            x += 1
            if registration_date in ('', None):
                log.debug("No registration date for {}".format(registration_id))
                continue
            registration_date = format_date(registration_date)

            registration_date_type = row[x]  # 2
            x += 1
            if registration_date_type in ('', None):
                registration_date_type = ''

            registration_end_status = row[x]  # 3
            x += 1
            if registration_end_status in ('', None):
                registration_end_status = ''

            transplant_consideration = row[x]  # 4
            x += 1
            if transplant_consideration in ('', None):
                transplant_consideration = ''

            ukt_suspension = row[x]  # 5
            x += 1
            if ukt_suspension in('', None):
                ukt_suspension = ''

            registration_end_date = row[x]  # 6
            x += 1
            if registration_end_date in ('', None):
                registration_end_date = None
            else:
                registration_end_date = format_date(registration_end_date)

            transplant_id = row[x]  # 7
            x += 1
            if transplant_id in ('', None):
                transplant_id = None
            else:
                transplant_id = int(transplant_id)

            transplant_list.append(registration_id)

            transplant_date = row[x]  # 8
            x += 1
            if transplant_date in ('', None):
                transplant_date = None
            else:
                transplant_date = format_date(transplant_date)


            transplant_type = row[x]  # 9
            x += 1
            if transplant_type in ('', None):
                transplant_type = ''

            transplant_sex = row[x]  # 10
            x += 1
            if transplant_sex in ('', None):
                transplant_sex = ''

            transplant_relationship = row[x]  # 11
            x += 1
            if transplant_relationship in ('', None):
                transplant_relationship = ''

            transplant_organ = row[x]  # 12
            x += 1
            if transplant_organ in ('', None):
                transplant_organ = ''

            transplant_unit = row[x]  # 13
            x += 1
            if transplant_unit in ('', None):
                transplant_unit = ''

            ukt_fail_date = row[x]  # 14
            x += 1
            if ukt_fail_date in ('', None):
                ukt_fail_date = None
            else:
                ukt_fail_date = format_date(ukt_fail_date)

            transplant_dialysis = row[x]  # 15
            x += 1
            if transplant_dialysis in ('', None):
                transplant_dialysis = ''

            cit_mins = row[x]  # 16
            x += 1
            if cit_mins in ('', None):
                cit_mins = ''

            hla_mismatch = row[x]  # 17
            x += 1
            if hla_mismatch in ('', None):
                hla_mismatch = ''

            cause_of_failure = row[x]  # 18
            x += 1
            if cause_of_failure in ('', None):
                cause_of_failure = ''

            cause_of_failure_text = row[x]  # 19
            x += 1
            if cause_of_failure_text in ('', None):
                cause_of_failure_text = ''

            results = Session.query(UKT_Transplant).filter_by(registration_id=registration_id).all()

            # Record exists - update it
            if len(results) > 0:

                ukt_transplant = results[0]
                log.info("Updating record")
                # No need to update Registration ID as it was used
                # for matching. Or UKTSSA_No as they're related.

                if (rr_no != ukt_transplant.rr_no):
                    ukt_transplant.rr_no = rr_no

                if registration_date != ukt_transplant.registration_date:
                    ukt_transplant.registration_date = registration_date

                if registration_date_type != ukt_transplant.registration_date_type:
                    ukt_transplant.registration_date_type = registration_date_type

                if registration_end_status != ukt_transplant.registration_end_status:
                    ukt_transplant.registration_end_status = registration_end_status

                if transplant_consideration != ukt_transplant.transplant_consideration:
                    ukt_transplant.transplant_consideration = transplant_consideration

                if ukt_suspension != ukt_transplant.ukt_suspension:
                    ukt_transplant.ukt_suspension = ukt_suspension

                if registration_end_date != ukt_transplant.registration_end_date:
                    if ukt_transplant.registration_end_date is not None:
                        excel_error_wb.Sheets['Transplant Field Differences'].WriteRow(
                            (
                                uktssa_no,
                                registration_id,
                                "Registration End Date",
                                registration_end_date,
                                ukt_transplant.registration_end_date
                            )
                        )
                    ukt_transplant.registration_end_date = registration_end_date

                if transplant_id != ukt_transplant.transplant_id:
                    ukt_transplant.transplant_id = transplant_id

                if transplant_date != ukt_transplant.transplant_date:
                    ukt_transplant.transplant_date = transplant_date

                if transplant_type != ukt_transplant.transplant_type:
                    ukt_transplant.transplant_type = transplant_type

                if transplant_sex != ukt_transplant.transplant_sex:
                    ukt_transplant.transplant_sex = transplant_sex

                if transplant_relationship != ukt_transplant.transplant_relationship:
                    ukt_transplant.transplant_relationship = transplant_relationship

                if transplant_organ != ukt_transplant.transplant_organ:
                    ukt_transplant.transplant_organ = transplant_organ

                # TODO: This might benefit from all being converted to ASCII
                if transplant_unit != ukt_transplant.transplant_unit:
                    ukt_transplant.transplant_unit = transplant_unit

                if ukt_fail_date != ukt_transplant.ukt_fail_date:
                    ukt_transplant.ukt_fail_date = ukt_fail_date

                if transplant_dialysis != ukt_transplant.transplant_dialysis:
                    ukt_transplant.transplant_dialysis = transplant_dialysis

                if cit_mins != ukt_transplant.cit_mins:
                    ukt_transplant.cit_mins = cit_mins

                if hla_mismatch != ukt_transplant.hla_mismatch:
                    ukt_transplant.hla_mismatch = hla_mismatch

                if cause_of_failure != ukt_transplant.cause_of_failure:
                    ukt_transplant.cause_of_failure = cause_of_failure

                if cause_of_failure_text != ukt_transplant.cause_of_failure_text:
                    ukt_transplant.cause_of_failure_text = cause_of_failure_text

            # Mew Record
            else:
                log.info("Add record to database")
                ukt_transplant = UKT_Transplant(
                    uktssa_no=uktssa_no,
                    rr_no=rr_no,
                    registration_id=registration_id,
                    registration_date=registration_date,
                    registration_date_type=registration_date_type,
                    registration_end_status=registration_end_status,
                    transplant_consideration=transplant_consideration,
                    ukt_suspension=ukt_suspension,
                    registration_end_date=registration_end_date,
                    transplant_id=transplant_id,
                    transplant_date=transplant_date,
                    transplant_type=transplant_type,
                    transplant_sex=transplant_sex,
                    transplant_relationship=transplant_relationship,
                    transplant_organ=transplant_organ,
                    transplant_unit=transplant_unit,
                    ukt_fail_date=ukt_fail_date,
                    transplant_dialysis=transplant_dialysis,
                    cit_mins=cit_mins,
                    hla_mismatch=hla_mismatch,
                    cause_of_failure=cause_of_failure,
                    cause_of_failure_text=cause_of_failure_text,
                )
                Session.add(ukt_transplant)

    Session.commit()

    Cursor = Engine.connect()

    sql_string = """
    SELECT
        DISTINCT UKTSSA_NO, RR_NO
    FROM
        UKT_PATIENTS
    WHERE
        RR_NO IS NOT NULL"""

    results = Cursor.execute(sql_string).fetchall()

    missing_patient_count = 0
    for row in results:
        if not (row[0] in patient_list):
            missing_patient_count = missing_patient_count + 1
            excel_error_wb.Sheets['Missing Patients'].WriteRow((row[0], row[1]))

    log.warning("Missing Prior UKT Patients {}".format(missing_patient_count))

    sql_string = """
    SELECT
        DISTINCT REGISTRATION_ID
    FROM
        UKT_TRANSPLANTS
    WHERE
        TRANSPLANT_ID IS NOT NULL AND
        RR_NO IS NOT NULL AND
        RR_NO < 999900000
    """

    results = Cursor.execute(sql_string).fetchall()

    transplant_list = set(transplant_list)
    # TODO: For Subsequent updates it may make sense to look for missing registrations
    for row in results:
        if row[0] not in transplant_list:
            log.warning("Missing Transplant {}".format(row[0]))
            excel_error_wb.Sheets['Missing Transplants'].WriteRow((row[0], ))
    log.info("Complete error spreadsheet {}".format(error_file))
    excel_error_wb.Save(error_file)


def main():
    logging.config.dictConfig(yaml.load(open('logconf.yaml')))
    log = logging.getLogger('ukt_import')
    parser = argparse.ArgumentParser(description="ukt_import")
    parser.add_argument('--input', type=str, help="Specify Input File")
    args = parser.parse_args()
    input_file = args.input
    if not len(sys.argv) > 1:
        parser.print_help()
        return
    if not os.path.exists(input_file):
        log.fatal("File {} must exist".format(input_file))
        sys.exit(1)
    with open(input_file) as c:
        csv_reader = csv.reader(c)
        folder, fn = os.path.split(input_file)
        error_fp = os.path.join(folder, 'UKT_Errors.xls')
        next(csv_reader)
        run(csv_reader, error_file=error_fp)


if __name__ == '__main__':
    main()
