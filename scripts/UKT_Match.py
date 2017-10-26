import csv
import time
import os
import sys

from rr_database.sqlserver import SQLServerDatabase
from rr_common.rr_general_utils import rr_str
from rr_common.general_exceptions import Error
from rr_common.nhs_numbers import RR_Validate_NHS_No
from rr_ukt_import.dateutils import convert_datetime_string_to_datetime
from datetime import datetime
from rr.utils.command_line import DBConnectionInfo
import logging
import logging.config
import yaml
import argparse

PAEDS_CSV = "1 Complete Database.csv"
INPUT_FILENAME = "UKTR_DATA_12JAN2017.csv"
OUTPUT_FILENAME = "UKTR_DATA_12JAN2017_MATCHED.csv"


def create_patients_table(db):
    sql = """
        SELECT *
        INTO #UKT_MATCH_PATIENTS
        FROM VWE_UKT_RR_PATIENTS;
    """
    db.execute(sql)

    sql = "CREATE NONCLUSTERED INDEX IDX_RR_NO ON #UKT_MATCH_PATIENTS (RR_NO)"
    db.execute(sql)

    sql = "CREATE NONCLUSTERED INDEX IDX_UNDELETED_RR_NO ON #UKT_MATCH_PATIENTS (UNDELETED_RR_NO)"
    db.execute(sql)


def main():
    logging.config.dictConfig(yaml.load(open('logconf.yaml', 'r')))
    log = logging.getLogger('ukt_match')
    parser = argparse.ArgumentParser(description="ukt_match")
    DBConnectionInfo.add_db_arguments(parser)
    parser.add_argument('--root', type=str, help="Specify Root Folder", required=True)
    parser.add_argument('--date', type=str, help="ddMMMYYY", required=True)
    parser.add_argument('--output', type=str, help="Specify alternate output")
    args = parser.parse_args()
    root = args.root
    datasource = 'RR-SQL-Live'
    catalog = 'RenalReg'
    if (args.datasource):
        datasource = args.datasource
    if (args.catalog):
        catalog = args.catalog
    db = SQLServerDatabase.connect(data_source=datasource, database=catalog)
    create_patients_table(db)
    rr_no_postcode_map = {}
    nhs_no_map = {}
    chi_no_map = {}
    hsc_no_map = {}
    uktssa_no_map = {}

    paeds_csv = os.path.join(args.root, PAEDS_CSV)
    if os.path.exists(paeds_csv):
        log.info("importing paeds patients from {} into the db...".format(paeds_csv))
        import_paeds_from_csv(db, paeds_csv, rr_no_postcode_map)
    else:
        log.critical("{} does not exist".format(paeds_csv))
        sys.exit(1)
    log.info("building postcode map...")
    populate_rr_no_postcode_map(db, rr_no_postcode_map)
    log.info("building identifier map...")
    populate_identifier_maps(db, nhs_no_map, chi_no_map, hsc_no_map, uktssa_no_map)

    log.info("matching patients...")

    ukt_columns = [
        "RR_ID", "UKTR_ID", "UKTR_TX_ID1", "UKTR_TX_ID2", "UKTR_TX_ID3", "UKTR_TX_ID4", "UKTR_TX_ID5", "UKTR_TX_ID6",
        "PREVIOUS_MATCH", "UKTR_RSURNAME", "UKTR_RFORENAME", "UKTR_RDOB", "UKTR_RSEX", "UKTR_RPOSTCODE", "UKTR_RNHS_NO",
    ]
    rr_columns = ["RR_ID", "RR_SURNAME", "RR_FORENAME", "RR_DOB", "RR_SEX", "RR_POSTCODE", "RR_NHS_NO"]
    input_filename = os.path.join(root, "UKTR_DATA_{}.csv".format(args.date))
    if not os.path.exists(input_filename):
        log.critical("Input filename {} does not exist".format(input_filename))
        sys.exit(1)
    reader = csv.reader(open(input_filename, "rb"))
    #
    if args.output:
        root = os.path.expanduser(args.output)
    output_filename = os.path.join(root, "UKTR_DATA_{}_MATCHED.csv".format(args.date))
    writer = csv.writer(open(output_filename, "wb"))

    columns = next(reader)
    check_columns(columns, ukt_columns)

    combined_columns = ukt_columns + rr_columns
    writer.writerow(combined_columns)

    log.info("Start Matching run")
    start_run = time.clock()
    for line_number, row in enumerate(reader, start=1):
        if line_number % 1000 == 0:
            log.info("line %d (%.2f/s)" % (line_number, line_number / (time.clock() - start_run)))

        pad_row(row, len(ukt_columns), fill="")

        uktssa_no = int(row[1])

        # Note: UKT put NHS no and CHI no in the same column
        nhs_no = row[14]
        chi_no = None
        hsc_no = None
        if nhs_no != '':
            number_type = RR_Validate_NHS_No(int(nhs_no))
            if number_type == 3:
                chi_no = nhs_no
                nhs_no = None
            if number_type == 4:
                hsc_no = nhs_no
                nhs_no = None
        else:
            nhs_no = None

        identifier_matches = []

        # Match by NHS no
        if nhs_no:
            nhs_no_match = nhs_no_map.get(nhs_no, None)

            if nhs_no_match:
                identifier_matches.append(nhs_no_match)

        # Match by CHI no
        if chi_no:
            chi_no_match = chi_no_map.get(chi_no, None)

            if chi_no_match:
                identifier_matches.append(chi_no_match)

        # Match by HSC no
        if hsc_no:
            hsc_no_match = hsc_no_map.get(hsc_no, None)

            if hsc_no_match:
                identifier_matches.append(hsc_no_match)

        # Match by UKTSSA no
        uktssa_no_match = uktssa_no_map.get(uktssa_no, None)

        if uktssa_no_match:
            identifier_matches.append(uktssa_no_match)

        matched_rr_nos = set(x[0] for x in identifier_matches)

        # A single patient matched
        if len(matched_rr_nos) == 1:
            rr_row = identifier_matches[0]

            rr_no = rr_row[0]

            # Format DOB
            rr_row[3] = rr_str(rr_row[3])

            # Populate postcode
            rr_row[5] = rr_no_postcode_map.get(rr_no, None)

            row.extend(rr_row)
        else:
            rr_no = None
            hosp_centre = None
            local_hosp_no = None
            scot_reg_no = None
            rr_only = "Y"
            include_deleted = "Y"

            surname = row[9]
            forename = row[10]
            postcode = row[13]

            dob = row[11]

            if dob != "":
                dob = convert_datetime_string_to_datetime(dob)
            else:
                dob = None

            params = [
                surname,
                forename,
                dob,
                rr_no,
                nhs_no,
                chi_no,
                hsc_no,
                uktssa_no,
                hosp_centre,
                local_hosp_no,
                scot_reg_no,
                postcode,
                rr_only,
                include_deleted
            ]

            db.cursor.callproc("PROC_UKT_MATCH_PATIENT_MATCHING", params)
            results = db.fetchall()
            # Found a match
            if len(results) > 0:
                result = results[0]

                rr_no = result[0]
                surname = result[3]
                forename = result[2]
                dob = rr_str(result[4])
                sex = get_patient_sex(db, result[0])
                nhs_no = result[5]

                # Postcode will be missing from the result as we aren't supplying a value for hosp_centre and this is
                # used to join the residency table
                postcode = rr_no_postcode_map.get(rr_no, "")

                row.extend([rr_no, surname, forename, dob, sex, postcode, nhs_no])

        # Ensure correct number of output columns
        pad_row(row, len(combined_columns))

        prev_match_rr_no = None

        try:
            prev_match_rr_no = int(row[0])
        except ValueError:
            pass

        match_rr_no = row[15]

        if prev_match_rr_no is None:
            # Didn't match last time
            prev_match = 0

            if match_rr_no:
                # But matched this time
                log.info("NEW_MATCH: UKTR_ID={} RR_NO={}".format(uktssa_no, match_rr_no))
        elif match_rr_no is None:
            # Didn't match this time
            prev_match = 3
            log.info("USED_TO_MATCH: UKTR_ID={} PREV_RR_NO={}".format(uktssa_no, prev_match_rr_no))
        elif prev_match_rr_no == match_rr_no:
            # Matched to the same patient
            prev_match = 1
        else:
            # Matched to a different patient
            prev_match = 2
            log.info("DIFFERENT_MATCH: UKTR_ID={} PREV_RR_NO={} RR_NO={}".format(uktssa_no, prev_match_rr_no, match_rr_no))

        row[8] = prev_match

        writer.writerow(row)
    log.info("Finish matching run")


def check_columns(columns, expected_columns):
    """ Check the column headings are as expected """

    if len(columns) != len(expected_columns):
        raise Error("expected %d columns, got %d" % (len(expected_columns), len(columns)))

    for i, (expected, actual) in enumerate(zip(expected_columns, columns), start=1):
        if expected != actual:
            raise Error('expected column %d to be "%s" not "%s"' % (i, expected, actual))


def import_paeds_from_csv(db, filename, rr_no_postcode_map):
    """ Import paeds patients into a temporary table """

    reader = csv.reader(open(filename, "rb"))

    columns = next(reader)
    paeds_columns = ["BAPN_No", "NHS_No", "CHI_No", "Renal_Reg_No", "UKT_no", "Surname", "Forename", "DOB", "Sex", "Postcode"]
    check_columns(columns, paeds_columns)

    dummy_rr_no = 999900001

    for line, row in enumerate(reader, start=1):
        rr_no = row[3]

        if rr_no == "":
            rr_no = dummy_rr_no
            dummy_rr_no += 1
        else:
            # Patient already in database
            continue

        bapn_no = row[0]

        nhs_no = row[1]

        if nhs_no == "":
            nhs_no = None

        chi_no = row[2]

        if chi_no == "":
            chi_no = None

        hsc_no = None
        if nhs_no:
            number_type = RR_Validate_NHS_No(int(nhs_no))
            if number_type == 4:
                hsc_no = nhs_no
                nhs_no = None

        uktssa_no = row[4]

        surname = row[5].upper()
        forename = row[6].upper()

        dob = row[7]
        dob = datetime.strptime(dob, "%d/%m/%Y")

        sex = row[8]

        if sex != "":
            if sex.lower() == "female":
                sex = 1
            elif sex.lower() == "male":
                sex = 2
            else:
                try:
                    sex = int(sex)
                except:
                    pass

                if sex not in [1, 2, 8]:
                    raise Error("unknown sex: %s" % sex)
        else:
            sex = 8

        patients_sql = """
            INSERT INTO #UKT_MATCH_PATIENTS (
                UNDELETED_RR_NO,
                RR_NO,
                UKTSSA_NO,
                SURNAME,
                FORENAME,
                DATE_BIRTH,
                NEW_NHS_NO,
                CHI_NO,
                HSC_NO,
                LOCAL_HOSP_NO,
                SOUNDEX_SURNAME,
                SOUNDEX_FORENAME,
                PATIENT_TYPE
            )
            VALUES (
                :RR_NO,
                :RR_NO,
                :UKTSSA_NO,
                :SURNAME,
                :FORENAME,
                :DATE_BIRTH,
                :NEW_NHS_NO,
                :CHI_NO,
                :HSC_NO,
                :BAPN_NO,
                SOUNDEX(dbo.normalise_surname2(:SURNAME)),
                SOUNDEX(dbo.normalise_forename2(:FORENAME)),
                'PAEDIATRIC'
            )
        """

        db.execute(patients_sql, {
            "BAPN_NO": bapn_no,
            "NEW_NHS_NO": nhs_no,
            "CHI_NO": chi_no,
            "HSC_NO": hsc_no,
            "RR_NO": rr_no,
            "UKTSSA_NO": uktssa_no,
            "SURNAME": surname,
            "FORENAME": forename,
            "DATE_BIRTH": dob,
            "SEX": sex,
            "LOCAL_HOSP_NO": bapn_no,
        })

        postcode = row[9]

        if postcode != "":
            rr_no_postcode_map[rr_no] = postcode


def populate_rr_no_postcode_map(db, rr_no_postcode_map):
    """ Build mapping between RR no and latest postcode """

    sql = """
        SELECT
            RR_NO,
            POST_CODE
        FROM
        (
            SELECT
                RR_NO,
                POST_CODE,
                ROW_NUMBER() OVER (PARTITION BY RR_NO ORDER BY DATE_START DESC) AS ROWNUMBER
            FROM
                RESIDENCY
        ) X
        WHERE
            ROWNUMBER = 1
    """

    db.execute(sql)

    for rr_no, postcode in db.fetchall():
        rr_no_postcode_map[rr_no] = postcode


def populate_identifier_maps(db, nhs_no_map, chi_no_map, hsc_no_map, uktssa_no_map):
    """ Build mappings from identifiers to RR columns for output """

    sql = """
        SELECT
            NEW_NHS_NO,
            CHI_NO,
            HSC_NO,
            UKTSSA_NO,
            RR_NO,
            SURNAME,
            FORENAME,
            DATE_BIRTH,
            SEX,
            NULL,
            NEW_NHS_NO
        FROM
            PATIENTS
    """

    db.execute(sql)

    for row in db.fetchall():
        nhs_no = row[0]
        chi_no = row[1]
        hsc_no = row[2]
        uktssa_no = row[3]
        patient = list(row[4:])

        if nhs_no:
            nhs_no_map[nhs_no] = patient

        if chi_no:
            chi_no_map[chi_no] = patient

        if hsc_no:
            hsc_no_map[hsc_no] = patient

        if uktssa_no:
            uktssa_no_map[uktssa_no] = patient

    return nhs_no_map, chi_no_map, uktssa_no_map


def get_patient_sex(db, rr_no):
    """ Get a patient's sex """

    for table in ["PATIENTS", "DELETED_PATIENTS"]:
        sql = "SELECT SEX FROM %s WHERE RR_NO = :RR_NO" % table
        db.execute(sql, {"RR_NO": rr_no})

        row = db.fetchone()

        if row:
            return row[0]

    return None


def pad_row(row, n_columns, fill=None):
    """ Pad a row to the desired number of columns """

    n_missing_columns = n_columns - len(row)

    if n_missing_columns > 0:
        row.extend([fill] * n_missing_columns)
    elif n_missing_columns < 0:
        raise Error("outputted too many columns")


if __name__ == "__main__":
    main()