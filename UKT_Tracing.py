import csv
from datetime import datetime

from rr.tracing_utils import NHSTracingBatch, next_nhs_tracing_batch_number
from rr.database.sqlserver import SQLServerDatabase

db = SQLServerDatabase.connect()

INPUT_FILENAME = r"C:\Users\nbj2301\Downloads\uktr_data_12JAN2015\UKTR_DATA_12JAN2015.csv"
batch_number = next_nhs_tracing_batch_number(db)
output_filename = "Tracing_%d.csv" % batch_number

input_f = open(INPUT_FILENAME, "rb")

# Remove headers from input
input_f.readline()

reader = csv.reader(input_f)

output_rows = []

for row in reader:
    if len(row) < 15:
        row.extend((15 - len(row)) * [""])

    uktssa_no = int(row[1])

    nhs_no = row[14]

    if nhs_no:
        try:
            nhs_no = int(nhs_no)
        except ValueError:
            nhs_no = None

    date_of_birth = row[11]

    if date_of_birth:
        try:
            date_of_birth = datetime.strptime(date_of_birth, "%d%b%Y").strftime("%Y%m%d")
        except ValueError:
            date_of_birth = None

    surname = row[9]
    forename = row[10]
    postcode = row[13]
    sex = row[12]

    output_row = [
        10,
        uktssa_no,
        date_of_birth,
        None,
        None,
        nhs_no,
        surname,
        None,
        forename,
        None,
        sex,
        None,
        None,
        None,
        None,
        None,
        postcode,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]

    output_rows.append(output_row)

batch = NHSTracingBatch(batch_number, len(output_rows))

output_f = open(output_filename, "wb")
writer = csv.writer(output_f)

output_f.write(batch.header() + "\n")

for output_row in output_rows:
    writer.writerow(output_row)

output_f.write(batch.footer() + "\n")

print(output_filename)
