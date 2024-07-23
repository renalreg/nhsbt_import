import re
import os
import csv
from nhsbt_import.utils import get_input_file_path, args_parse

args = args_parse()


def count_special_characters(cell_value):
    if isinstance(cell_value, str):
        null_byte_count = cell_value.count("\x00")
        non_ascii_count = len(re.findall(r"[^\x00-\x7F]", cell_value))
        return null_byte_count, non_ascii_count

    return 0, 0


def process_csv(input_filename, output_filename):
    total_null_bytes = 0
    total_non_ascii = 0

    with open(input_filename, newline="", encoding="utf-8", errors="replace") as infile:
        reader = csv.reader(infile)
        rows = list(reader)

        for row in rows:
            for cell in row:
                null_bytes, non_ascii = count_special_characters(cell)
                total_null_bytes += null_bytes
                total_non_ascii += non_ascii

        cleaned_rows = []
        for row in rows:
            cleaned_row = [clean_cell_value(cell) for cell in row]
            cleaned_rows.append(cleaned_row)

    with open(output_filename, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(cleaned_rows)

    return total_null_bytes, total_non_ascii


def clean_cell_value(cell_value):
    if isinstance(cell_value, str):
        return re.sub(r"[^\x00-\x7F]", "", cell_value.replace("\x00", ""))
    return cell_value


def check_output(output_filepath):
    total_null_bytes = 0
    total_non_ascii = 0

    with open(
        output_filepath, newline="", encoding="utf-8", errors="replace"
    ) as outfile:
        reader = csv.reader(outfile)
        rows = list(reader)

        for row in rows:
            for cell in row:
                null_bytes, non_ascii = count_special_characters(cell)
                total_null_bytes += null_bytes
                total_non_ascii += non_ascii

    return total_null_bytes, total_non_ascii


input_file = get_input_file_path(args.directory)
output_file = os.path.join(args.directory, "no_null_bytes.csv")

total_null_bytes_before, total_non_ascii_before = process_csv(input_file, output_file)

print(f"Total ASCII null bytes before clean: {total_null_bytes_before}")
print(f"Total non-ASCII characters before clean: {total_non_ascii_before}")

total_null_bytes_after, total_non_ascii_after = check_output(output_file)

print(f"Total ASCII null bytes after clean: {total_null_bytes_after}")
print(f"Total non-ASCII characters after clean: {total_non_ascii_after}")
