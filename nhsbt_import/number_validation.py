import os
import pandas as pd

from nhsbt_import.utils import validate_and_correct_nhs_numbers, log


def split_valid_and_invalid_rows(input_file_path: str) -> str:
    """
    Reads input_file_path, runs validate_and_correct_nhs_numbers over every row,
    and splits the data into two CSVs written alongside the input file:
        - <name>_failed.csv: rows that raised a ValueError, annotated with the
          originating CSV line number and the error message.
        - <name>_passed.csv: rows that passed (or were successfully corrected).

    Returns the path to the passed CSV, which should be used as the input
    for the rest of the import pipeline.
    """
    nhsbt_df = pd.read_csv(
        input_file_path,
        na_filter=False,
        skip_blank_lines=True,
    )
    nhsbt_df = nhsbt_df.rename(columns={"uktr_rsex": "UKTR_RSEX"})

    passed_rows = []
    failed_rows = []

    for row_index, row in nhsbt_df.iterrows():
        # +2 accounts for 0-based index and the header row, matching the
        # actual line number in the source CSV file.
        csv_line_number = row_index + 2

        try:
            corrected_row = validate_and_correct_nhs_numbers(row.copy(), row_index)
            passed_rows.append(corrected_row)
        except ValueError as e:
            failed_row = row.copy()
            failed_row["csv_line_number"] = csv_line_number
            failed_row["error_message"] = str(e)
            failed_rows.append(failed_row)

    passed_df = (
        pd.DataFrame(passed_rows)
        if passed_rows
        else pd.DataFrame(columns=nhsbt_df.columns)
    )
    failed_df = (
        pd.DataFrame(failed_rows)
        if failed_rows
        else pd.DataFrame(columns=list(nhsbt_df.columns) + ["csv_line_number", "error_message"])
    )

    base, ext = os.path.splitext(input_file_path)
    failed_output_path = f"{base}_invalid_numbers{ext}"
    passed_output_path = f"{base}_valid_numbers{ext}"

    failed_df.to_csv(failed_output_path, index=False)
    passed_df.to_csv(passed_output_path, index=False)

    if not failed_df.empty:
        bad_lines = ", ".join(str(x) for x in failed_df["csv_line_number"].tolist())
        log.error(
            f"{len(failed_df)} row(s) failed NHS number validation and were "
            f"excluded from import. Lines: {bad_lines}. See {failed_output_path}"
        )

    return passed_output_path