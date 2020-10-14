import csv
from datetime import datetime
import logging
import logging.config
import yaml
import argparse
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from UKT_Models import UKT_Patient, UKT_Transplant
from RR_Models import RR_Patient, RR_Deleted_Patient
from rr_reports import ExcelLib


def format_date(str_date):
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


def run(csv_reader, error_file='UKT_Errors.xls'):
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

    TheExcelErrorWB = ExcelLib.ExcelWB()
    TheExcelErrorWB.AddSheet(
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
    TheExcelErrorWB.AddSheet(
        "Patient Field Differences",
        ("UKTSSA_No", "Field", "File Value", "Previous Import Value"),
        0)
    TheExcelErrorWB.AddSheet(
        'Transplant Field Differences',
        ("UKTSSA_No", "Transplant_ID", "Field", "File Value", "Previous Import Value"),
        0)
    TheExcelErrorWB.AddSheet("Invalid Postcodes", ("UKTSSA_No", "Message", "Value"), 0)
    TheExcelErrorWB.AddSheet("Invalid NHS Numbers", ("UKTSSA_No", "Value"), 0)
    TheExcelErrorWB.AddSheet("Missing Patients", ("UKTSSA_No", ), 0)
    TheExcelErrorWB.AddSheet("Missing Transplants", ("Transplant_ID", ), 0)

    PatientList = list()
    TransplantList = list()

    for line_number, Row in enumerate(csv_reader, start=2):
        log.info("on line {}".format(line_number))
        Row = list(Row)
        for i in range(0, len(Row)):
            if isinstance(Row[i], str):
                Row[i] = Row[i]

        # Empty Strings are needed in places here as that's what SQLAlachemy
        # appears to be returning as Null for String fields

        UKTSSA_No = int(Row[0].strip())
        if UKTSSA_No in (0, ''):
            UKTSSA_No = None
        else:
            UKTSSA_No = int(UKTSSA_No)
            PatientList.append(UKTSSA_No)

        RR_No = Row[1].replace('/', '').strip()
        if RR_No in ('', 0):
            RR_No = None
        else:
            RR_No = int(RR_No)

        # Skip paed patients
        if str(RR_No)[:4] == "9999":
            continue
        # Skip Q100 new patients
        elif str(RR_No)[:4] == "8888":
            continue

        Surname = None
        Forename = None
        Sex = None
        Post_Code = None
        New_NHS_No = None

        ukt_date_death = Row[2]
        if ukt_date_death in ('', 0):
            ukt_date_death = None
        else:
            ukt_date_death = format_date(ukt_date_death)

        UKT_Date_Birth = None

        Results = Session.query(UKT_Patient).filter_by(UKTSSA_No=UKTSSA_No).all()

        if len(Results) == 1:
            log.info("UKT Patient {} found in database".format(UKTSSA_No))
            TheUKTPatient = Results[0]
            log.info("Updating record")
            if Surname != TheUKTPatient.Surname:
                TheUKTPatient.Surname = Surname
            if Forename != TheUKTPatient.Forename:
                TheUKTPatient.Forename = Forename
            if Sex != TheUKTPatient.Sex:
                TheUKTPatient.Sex = Sex
            if Post_Code != TheUKTPatient.Post_Code:
                TheUKTPatient.Post_Code = Post_Code
            if New_NHS_No != TheUKTPatient.New_NHS_No:
                TheUKTPatient.New_NHS_No = New_NHS_No
            if ukt_date_death != TheUKTPatient.UKT_Date_Death:
                TheUKTPatient.ukt_date_death = ukt_date_death
            if UKT_Date_Birth != TheUKTPatient.UKT_Date_Birth:
                TheUKTPatient.UKT_Date_Birth = UKT_Date_Birth

            MatchType = None
            # RR_No here comes from the matched file
            if RR_No != TheUKTPatient.RR_No \
                    and (TheUKTPatient.RR_No is not None or RR_No is not None):
                MatchType = "Match Difference"
            elif RR_No is not None and TheUKTPatient.RR_No is None:
                # This should never happen as we now can't load the unmatched records.
                MatchType = "New Match"
            try:
                TheRRPatient = Session.query(RR_Patient).filter_by(RR_No=RR_No).all()[0]
            except Exception:
                try:
                    TheRRPatient = Session.query(RR_Deleted_Patient).filter_by(RR_No=RR_No).all()[0]
                    MatchType = "Match to Deleted Patient"
                except Exception:
                    MatchType = "Match to Patient not in Database"

            if MatchType is not None:
                TheExcelErrorWB.Sheets['Match Differences'].WriteRow(
                    (
                        UKTSSA_No,
                        MatchType,
                        RR_No,
                        Surname,
                        Forename,
                        Sex,
                        UKT_Date_Birth,
                        New_NHS_No,
                        TheRRPatient.RR_No,
                        TheRRPatient.Surname,
                        TheRRPatient.Forename,
                        TheRRPatient.Sex,
                        TheRRPatient.Date_Birth,
                        TheRRPatient.New_NHS_No
                    )
                )
            # Update the RR_No
            if RR_No is not None and RR_No != TheUKTPatient.RR_No:
                TheUKTPatient.RR_No = RR_No
        elif len(Results) == 0:
            log.info("Add patient")
            ThePatient = UKT_Patient(
                UKTSSA_No=UKTSSA_No,
                Surname=Surname,
                Forename=Forename,
                Sex=Sex,
                Post_Code=Post_Code,
                New_NHS_No=New_NHS_No,
                RR_No=RR_No,
                UKT_Date_Death=ukt_date_death,
                UKT_Date_Birth=UKT_Date_Birth
            )
            Session.add(ThePatient)
        else:
            log.error("{} in the database multiple times".format(UKTSSA_No))

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

            Registration_ID = str(UKTSSA_No) + "_" + str(i + 1)

            Registration_Date = Row[x]  # 1
            x += 1
            if Registration_Date in ('', None):
                log.debug("No registration date for {}".format(Registration_ID))
                continue
            Registration_Date = format_date(Registration_Date)

            Registration_Date_Type = Row[x]  # 2
            x += 1
            if Registration_Date_Type in ('', None):
                Registration_Date_Type = ''

            Registration_End_Status = Row[x]  # 3
            x += 1
            if Registration_End_Status in ('', None):
                Registration_End_Status = ''

            Transplant_Consideration = Row[x]  # 4
            x += 1
            if Transplant_Consideration in ('', None):
                Transplant_Consideration = ''

            UKT_SUSPENSION = Row[x]  # 5
            x += 1
            if UKT_SUSPENSION in('', None):
                UKT_SUSPENSION = ''

            Registration_End_Date = Row[x]  # 6
            x += 1
            if Registration_End_Date in ('', None):
                Registration_End_Date = None
            else:
                Registration_End_Date = format_date(Registration_End_Date)

            Transplant_ID = Row[x]  # 7
            x += 1
            if Transplant_ID in ('', None):
                Transplant_ID = None
            else:
                Transplant_ID = int(Transplant_ID)

            TransplantList.append(Registration_ID)

            Transplant_Date = Row[x]  # 8
            x += 1
            if Transplant_Date in ('', None):
                Transplant_Date = None
            else:
                try:
                    Transplant_Date = format_date(Transplant_Date)
                except Exception:
                    raise

            Transplant_Type = Row[x]  # 9
            x += 1
            if Transplant_Type in ('', None):
                Transplant_Type = ''

            Transplant_Sex = Row[x]  # 10
            x += 1
            if Transplant_Sex in ('', None):
                Transplant_Sex = ''

            Transplant_Relationship = Row[x]  # 11
            x += 1
            if Transplant_Relationship in ('', None):
                Transplant_Relationship = ''

            Transplant_Organ = Row[x]  # 12
            x += 1
            if Transplant_Organ in ('', None):
                Transplant_Organ = ''

            Transplant_Unit = Row[x]  # 13
            x += 1
            if Transplant_Unit in ('', None):
                Transplant_Unit = ''

            UKT_Fail_Date = Row[x]  # 14
            x += 1
            if UKT_Fail_Date in ('', None):
                UKT_Fail_Date = None
            else:
                UKT_Fail_Date = format_date(UKT_Fail_Date)

            Transplant_Dialysis = Row[x]  # 15
            x += 1
            if Transplant_Dialysis in ('', None):
                Transplant_Dialysis = ''

            CIT_Mins = Row[x]  # 16
            x += 1
            if CIT_Mins in ('', None):
                CIT_Mins = ''

            HLA_Mismatch = Row[x]  # 17
            x += 1
            if HLA_Mismatch in ('', None):
                HLA_Mismatch = ''

            Cause_Of_Failure = Row[x]  # 18
            x += 1
            if Cause_Of_Failure in ('', None):
                Cause_Of_Failure = ''

            Cause_Of_Failure_Text = Row[x]  # 19
            x += 1
            if Cause_Of_Failure_Text in('', None):
                Cause_Of_Failure_Text = ''

            Results = Session.query(UKT_Transplant).filter_by(Registration_ID=Registration_ID).all()

            # Record exists - update it
            if len(Results) > 0:

                TheTransplant = Results[0]
                log.info("Updating record")
                # No need to update Registration ID as it was used
                # for matching. Or UKTSSA_No as they're related.

                if (RR_No != TheTransplant.RR_No):
                    TheTransplant.RR_No = RR_No

                if Registration_Date != TheTransplant.Registration_Date:
                    TheTransplant.Registration_Date = Registration_Date

                if Registration_Date_Type != TheTransplant.Registration_Date_Type:
                    TheTransplant.Registration_Date_Type = Registration_Date_Type

                if Registration_End_Status != TheTransplant.Registration_End_Status:
                    TheTransplant.Registration_End_Status = Registration_End_Status

                if Transplant_Consideration != TheTransplant.Transplant_Consideration:
                    TheTransplant.Transplant_Consideration = Transplant_Consideration

                if UKT_SUSPENSION != TheTransplant.UKT_SUSPENSION:
                    TheTransplant.UKT_SUSPENSION = UKT_SUSPENSION

                if Registration_End_Date != TheTransplant.Registration_End_Date:
                    if TheTransplant.Registration_End_Date is not None:
                        TheExcelErrorWB.Sheets['Transplant Field Differences'].WriteRow(
                            (
                                UKTSSA_No,
                                Registration_ID,
                                "Registration End Date",
                                Registration_End_Date,
                                TheTransplant.Registration_End_Date
                            )
                        )
                    TheTransplant.Registration_End_Date = Registration_End_Date

                if Transplant_ID != TheTransplant.Transplant_ID:
                    TheTransplant.Transplant_ID = Transplant_ID

                if Transplant_Date != TheTransplant.Transplant_Date:
                    TheTransplant.Transplant_Date = Transplant_Date

                if Transplant_Type != TheTransplant.Transplant_Type:
                    TheTransplant.Transplant_Type = Transplant_Type

                if Transplant_Sex != TheTransplant.Transplant_Sex:
                    TheTransplant.Transplant_Sex = Transplant_Sex

                if Transplant_Relationship != TheTransplant.Transplant_Relationship:
                    TheTransplant.Transplant_Relationship = Transplant_Relationship

                if Transplant_Organ != TheTransplant.Transplant_Organ:
                    TheTransplant.Transplant_Organ = Transplant_Organ

                # TODO: This might benefit from all being converted to ASCII
                if Transplant_Unit != TheTransplant.Transplant_Unit:
                    TheTransplant.Transplant_Unit = Transplant_Unit

                if UKT_Fail_Date != TheTransplant.UKT_Fail_Date:
                    TheTransplant.UKT_Fail_Date = UKT_Fail_Date

                if Transplant_Dialysis != TheTransplant.Transplant_Dialysis:
                    TheTransplant.Transplant_Dialysis = Transplant_Dialysis

                if CIT_Mins != TheTransplant.CIT_Mins:
                    TheTransplant.CIT_Mins = CIT_Mins

                if HLA_Mismatch != TheTransplant.HLA_Mismatch:
                    TheTransplant.HLA_Mismatch = HLA_Mismatch

                if Cause_Of_Failure != TheTransplant.Cause_Of_Failure:
                    TheTransplant.Cause_Of_Failure = Cause_Of_Failure

                if Cause_Of_Failure_Text != TheTransplant.Cause_Of_Failure_Text:
                    TheTransplant.Cause_Of_Failure_Text = Cause_Of_Failure_Text

            # Mew Record
            else:
                log.info("Add record to database")
                TheTransplant = UKT_Transplant(
                    UKTSSA_No=UKTSSA_No,
                    RR_No=RR_No,
                    Registration_ID=Registration_ID,
                    Registration_Date=Registration_Date,
                    Registration_Date_Type=Registration_Date_Type,
                    Registration_End_Status=Registration_End_Status,
                    Transplant_Consideration=Transplant_Consideration,
                    UKT_SUSPENSION=UKT_SUSPENSION,
                    Registration_End_Date=Registration_End_Date,
                    Transplant_ID=Transplant_ID,
                    Transplant_Date=Transplant_Date,
                    Transplant_Type=Transplant_Type,
                    Transplant_Sex=Transplant_Sex,
                    Transplant_Relationship=Transplant_Relationship,
                    Transplant_Organ=Transplant_Organ,
                    Transplant_Unit=Transplant_Unit,
                    UKT_Fail_Date=UKT_Fail_Date,
                    Transplant_Dialysis=Transplant_Dialysis,
                    CIT_Mins=CIT_Mins,
                    HLA_Mismatch=HLA_Mismatch,
                    Cause_Of_Failure=Cause_Of_Failure,
                    Cause_Of_Failure_Text=Cause_Of_Failure_Text,
                )
                Session.add(TheTransplant)

    Session.commit()

    Cursor = Engine.connect()

    SQLString = """
    SELECT
        DISTINCT UKTSSA_NO, RR_NO
    FROM
        UKT_PATIENTS
    WHERE
        RR_NO IS NOT NULL"""

    results = Cursor.execute(SQLString).fetchall()

    MissingPatientCount = 0
    for row in results:
        if not (row[0] in PatientList):
            MissingPatientCount = MissingPatientCount + 1
            TheExcelErrorWB.Sheets['Missing Patients'].WriteRow((row[0], row[1]))

    log.warning("Missing Prior UKT Patients {}".format(MissingPatientCount))

    SQLString = """
    SELECT
        DISTINCT REGISTRATION_ID
    FROM
        UKT_TRANSPLANTS
    WHERE
        TRANSPLANT_ID IS NOT NULL AND
        RR_NO IS NOT NULL AND
        RR_NO < 999900000
    """

    Results = Cursor.execute(SQLString).fetchall()

    TransplantList = set(TransplantList)
    # TODO: For Subsequent updates it may make sense to look for missing registrations
    for Row in Results:
        if Row[0] not in TransplantList:
            log.warning("Missing Transplant {}".format(Row[0]))
            TheExcelErrorWB.Sheets['Missing Transplants'].WriteRow((Row[0], ))
    log.info("Complete error spreadsheet {}".format(error_file))
    TheExcelErrorWB.Save(error_file)


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
