import os
import os.path
import csv
import xlrd

from typing import List, Dict
from datetime import datetime
from lxml import etree

from rr_rrtf.Convert import RRTF_Cracked_To_XML
from rr_common.rr_general_utils import LD
from ukrdc.database import Connection


def get_xml_value(parentnode, xpath: str) -> str:
    node = LD(parentnode.xpath(xpath))
    # node doesn't evaluate to True.
    if node is not None:
        value = node.text
        if value in ("", None):
            value = None
    else:
        value = None

    return value


def get_location_lookup() -> Dict:

    # TODO: Move to a utils file somewhere?

    renalreg_engine = Connection.get_engine_from_file(None, "ukrdc_rrsqllive")
    renalreg_connection = renalreg_engine.raw_connection()
    renalreg_cursor = renalreg_connection.cursor()

    sql_string = """
    SELECT
        CENTRE_CODE,
        COUNTRY_CODE
    FROM
        LOCATIONS
    """

    renalreg_cursor.execute(sql_string)
    results = renalreg_cursor.fetchall()

    location_lookup = dict()
    for row in results:
        location_lookup[row[0]] = row[1]

    return location_lookup


def process_cracked_file(filepath: str, demographics_only: bool = True) -> List:

    output_list = list()

    try:
        file = open(filepath, "r")
        xml_string = RRTF_Cracked_To_XML(file)
    except Exception:
        print("Error Parsing", filepath)
        raise Exception

    root = etree.fromstring(xml_string)

    for IDNNode in root.xpath("IDN"):
        rr_no = get_xml_value(IDNNode, "IDN00")
        if rr_no:
            rr_no = rr_no.replace("/", "")

        surname = get_xml_value(IDNNode, "IDN01")
        forename = get_xml_value(IDNNode, "IDN02")

        dob = get_xml_value(IDNNode, "IDN03")
        if dob:
            dob = datetime.strptime(dob, "%d/%m/%Y")

        sex = get_xml_value(IDNNode, "PAT/PAT00")

        local_hosp_no = get_xml_value(IDNNode, "IDN04")

        chi_no = get_xml_value(IDNNode, "PAT/PAT11")
        if chi_no:
            chi_no = chi_no.replace(" ", "")
            try:
                chi_no = int(chi_no)
            except Exception:
                chi_no = None
        nhs_no = get_xml_value(IDNNode, "PAT/PAT13")
        if nhs_no:
            nhs_no = nhs_no.replace(" ", "")
            try:
                nhs_no = int(nhs_no)
            except Exception:
                nhs_no = None
        hsc_no = get_xml_value(IDNNode, "PAT/PAT18")
        if hsc_no:
            hsc_no = hsc_no.replace(" ", "")
            try:
                hsc_no = int(hsc_no)
            except Exception:
                hsc_no = None

        output_row = [
            rr_no,
            surname,
            forename,
            sex,
            dob,
            local_hosp_no,
            chi_no,
            nhs_no,
            hsc_no,
        ]

        if not demographics_only:
            hosp_centre = get_xml_value(IDNNode, "PAT/PAT01")
            qua_modality = get_xml_value(IDNNode, "QUA/QUA02")
            qua_treat_sup = get_xml_value(IDNNode, "QUA/QUA05")
            # TODO: This should be the last. There is some code for this somewhere...
            qbl_modality = get_xml_value(IDNNode, "QBL/QBL02")
            if qbl_modality:
                qbl_modality = qbl_modality.replace("%code=", "")
            qbl_treat_sup = get_xml_value(IDNNode, "QBL/QBL05")
            prd_code = get_xml_value(IDNNode, "ERF/ERFAJ")
            if not prd_code:
                # Allow for old PRD Codes
                prd_code = get_xml_value(IDNNode, "ERF/ERF04")
            if prd_code:
                prd_code = prd_code.replace("%EDTA2=", "")
                try:
                    prd_code = str(int(prd_code))
                except Exception:
                    prd_code = None

            ethnicity = get_xml_value(IDNNode, "PAT/PAT25")
            if ethnicity:
                ethnicity = ethnicity.replace("%READ=", "")
                ethnicity = ethnicity.replace("%Code=", "")

            output_row.extend(
                [
                    hosp_centre,
                    qua_modality,
                    qua_treat_sup,
                    qbl_modality,
                    qbl_treat_sup,
                    prd_code,
                    ethnicity,
                ]
            )

        output_list.append(output_row)

    return output_list


def process_guys_csv(filepath: str, demographics_only: bool = True) -> List:

    csvreader = csv.reader(open(filepath, "r", newline=""))
    # Skip Headers
    next(csvreader)

    output_list = list()

    for row in csvreader:
        rr_no = row[1]
        if rr_no:
            rr_no = rr_no.replace("/", "")
            try:
                rr_no = int(rr_no)
            except Exception:
                rr_no = None
        surname = row[2]
        forename = row[3]
        dob = row[4]
        if dob:
            dob = datetime.strptime(dob, "%d/%m/%Y")
        local_hosp_no = row[5]

        chi_no = None
        try:
            nhs_no = int(row[0])
        except Exception:
            nhs_no = None
        hsc_no = None

        # TODO: Fran is to ask Guy's for a new sheet with this in.
        sex = None

        output_row = [
            rr_no,
            surname,
            forename,
            sex,
            dob,
            local_hosp_no,
            chi_no,
            nhs_no,
            hsc_no,
        ]

        if not demographics_only:
            # Note This is Treatment Centre.
            # I don't think we can default this to RJ121 as I think the
            # Paeds are in here too.
            hosp_centre = row[14]

            qua_modality = None
            qua_treat_sup = None
            qbl_modality = row[13]
            qbl_treat_sup = row[15]
            prd_code = None
            ethnicity = None

            output_row.extend(
                [
                    hosp_centre,
                    qua_modality,
                    qua_treat_sup,
                    qbl_modality,
                    qbl_treat_sup,
                    prd_code,
                    ethnicity,
                ]
            )

        output_list.append(output_row)

    return output_list


def process_bhly_xls(filepath: str, demographics_only: bool = True) -> List:

    output_list = list()

    workbook = xlrd.open_workbook(filepath)
    # Assume 4 sheets/book - PD/HD/Transplant/CKD

    for i in range(0, 4):
        worksheet = workbook.sheet_by_index(i)
        # Start at 1 to skip headers
        for row_index in range(1, worksheet.nrows):
            nhs_no = worksheet.cell(row_index, 1).value
            if nhs_no:
                try:
                    nhs_no = int(nhs_no)
                except Exception:
                    nhs_no = None
            rr_no = worksheet.cell(row_index, 2).value
            if rr_no:
                rr_no = rr_no.replace("/", "")
                try:
                    rr_no = int(rr_no)
                except Exception:
                    rr_no = None
            dob = worksheet.cell(row_index, 3).value
            if dob:
                dob_tuple = xlrd.xldate_as_tuple(dob, workbook.datemode)
                dob = datetime(*dob_tuple)
            sex = worksheet.cell(row_index, 7).value
            if sex == "M":
                sex = 1
            elif sex == "F":
                sex = 2

            output_row = [rr_no, None, None, sex, dob, None, None, nhs_no, None]

            if not demographics_only:

                hosp_centre = worksheet.cell(row_index, 5).value

                bhly_modality_map = {
                    "PD": "19",
                    "CHD": "9",
                    "HHD": "9",
                    "TPLT": "29",
                    "CKD": "900",
                    "CKDCOM": "900",
                }

                bhly_treat_sup_map = {"HHD": "HOME"}

                qua_modality = bhly_modality_map.get(
                    worksheet.cell(row_index, 4).value, None
                )

                if qua_modality == "HHD":
                    qua_treat_sup = "HOME"
                elif worksheet.cell(row_index, 6) not in ("", None):
                    # worksheet.cell(row_index, 4) has Treatment Centres if not
                    # treated at main unit.
                    qua_treat_sup = "SATL"
                else:
                    qua_treat_sup = "HOSP"

                qua_treat_sup = bhly_treat_sup_map.get(
                    worksheet.cell(row_index, 4).value, None
                )
                qbl_modality = None
                qbl_treat_sup = None

                prd_code = worksheet.cell(row_index, 12).value
                if prd_code:
                    try:
                        prd_code = str(int(prd_code))
                    except Exception:
                        prd_code = None
                # TODO: This needs to be converted from words to RR18.
                ethnicity = worksheet.cell(row_index, 8).value

                output_row.extend(
                    [
                        hosp_centre,
                        qua_modality,
                        qua_treat_sup,
                        qbl_modality,
                        qbl_treat_sup,
                        prd_code,
                        ethnicity,
                    ]
                )

            output_list.append(output_row)

    return output_list


def process(
    base_path: str,
    quarter_folder: str,
    demographics_only: bool = True,
    country: str = "ALL",
) -> List:

    output_list = list()

    unit_folders = os.listdir(base_path)

    # going to need to pass a cursor from main module
    location_lookup = get_location_lookup()

    for unit_folder in unit_folders:

        unit_code = unit_folder.split("-")[-1].strip()

        if unit_folder in ("Scotland",):
            print("Skipping", unit_folder)
        elif country != "ALL" and location_lookup.get(unit_code, "SKIP") != country:
            print("Skipping", unit_folder)
        else:
            if os.path.isdir(base_path + unit_folder):

                file_found = False

                # Guy's
                if unit_code in ("RJ121",):
                    filename = unit_code + "r2019401.csv"
                    filepath = os.path.join(
                        base_path, unit_folder, quarter_folder, filename
                    )
                    if os.path.exists(filepath):
                        file_found = True
                        print("Proccessing", filepath)
                        output_list.extend(
                            process_guys_csv(filepath, demographics_only)
                        )

                # BHLY
                # elif unit_code in ("RAE05", "RF201", "RQR13", "RCB55"):
                # filename = "2019-12-31 " + unit_code + ".xls"
                # filepath = os.path.join(
                # base_path, unit_folder, quarter_folder, filename
                # )
                # if os.path.exists(filepath):
                # file_found = True
                # print("Proccessing", filepath)
                # output_list.extend(
                # process_bhly_xls(filepath, demographics_only)
                # )

                # Normal UKRR Files
                else:
                    filename = unit_code + "r20194"
                    filepath = ""
                    for filename in (
                        filename + "01.txt",
                        filename + "02.txt",
                        filename + "03.txt",
                        filename + "01_Data.txt",
                        filename + "02_Data.txt",
                        filename + "03_Data.txt",
                    ):

                        filepath = os.path.join(
                            base_path, unit_folder, quarter_folder, filename
                        )

                        if os.path.exists(filepath):
                            file_found = True
                            break

                    if file_found:
                        print("Proccessing", filepath)
                        output_list.extend(
                            process_cracked_file(filepath, demographics_only)
                        )
                        continue

                if not file_found:
                    print("Couldn't find File -", unit_folder)

    return output_list


if __name__ == "__main__":
    process("Q:/SVNData/2019/", "Q100", country="GB-ENG")
