import datetime
import os
import sys
import warnings

import pandas as pd
from sqlalchemy import text
from ukrr_models.nhsbt_models import UKTPatient, UKTTransplant  # type: ignore
from ukrr_models.rr_models import UKRR_Deleted_Patient  # type: ignore

from nhsbt_import import utils

warnings.simplefilter(action="ignore", category=FutureWarning)
args = utils.args_parse()
log = utils.create_logs(args.directory)

def check_unit_names(session) -> pd.DataFrame:
    """
    Runs the unmatched TRANSPLANT_UNIT check against UKT_SITES.
    Returns a DataFrame of any unmatched unit names (empty if none).
    """
    query = text("""
        SELECT DISTINCT TRANSPLANT_UNIT
        FROM UKT_TRANSPLANTS
        WHERE TRANSPLANT_UNIT NOT IN (SELECT SITE_NAME FROM UKT_SITES)
        ORDER BY TRANSPLANT_UNIT
    """)
    result = session.execute(query)
    return pd.DataFrame(result.fetchall(), columns=result.keys())


def prompt_and_add_site_codes(session, unmatched_df: pd.DataFrame) -> None:
    """
    For each unmatched unit name, asks the user whether it's a genuine NHS
    hospital (as opposed to a private/foreign clinic, which is expected and
    should be ignored). If yes, prompts for the site code and inserts a new
    row into UKT_SITES.
    """
    if unmatched_df.empty:
        log.info("No unmatched TRANSPLANT_UNIT names found.")
        return

    log.info(f"{len(unmatched_df)} unmatched TRANSPLANT_UNIT name(s) found.")
    for unit_name in unmatched_df["TRANSPLANT_UNIT"]:
        print(f"\nUnmatched unit: {unit_name}")
        add = input("NHS hospital that should be added to UKT_SITES? (y/n): ").strip().lower()
        if add != "y":
            log.info(f"Skipping '{unit_name}' (assumed private/foreign clinic).")
            continue

        site_code = input(f"Enter SITE_CODE for '{unit_name}': ").strip()
        if not site_code:
            log.warning(f"No site code entered, skipping '{unit_name}'.")
            continue

        session.execute(
            text("INSERT INTO UKT_SITES (SITE_NAME, SITE_CODE) VALUES (:site_name, :site_code)"),
            {"site_name": unit_name, "site_code": site_code},
        )
        log.info(f"Added '{unit_name}' -> '{site_code}' to UKT_SITES.")

    session.commit()


def run_ukt_link_procedure(session) -> None:
    """Executes the PROC_UKT_LINK stored procedure on the renalreg database."""
    log.info("Executing PROC_UKT_LINK...")
    session.execute(text("EXEC PROC_UKT_LINK"))
    session.commit()
    log.info("PROC_UKT_LINK completed.")


def export_ukt_transplant_extract(session, output_directory: str) -> tuple[pd.DataFrame, str]:
    """
    Queries VWE_UKT_TRANSPLANT_EXTRACT_NEW and writes the results to CSV,
    replacing the manual SSMS 'copy with headers -> Excel -> Save As CSV' step.

    Returns (df, output_path) so downstream steps can reuse the data without
    re-querying.
    """
    result = session.execute(text("SELECT * FROM VWE_UKT_TRANSPLANT_EXTRACT_NEW"))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())

    output_path = os.path.join(output_directory, "ukt_transplant_extract_v1.csv")
    df.to_csv(output_path, index=False)
    log.info(f"Exported {len(df)} row(s) to {output_path}")
    return df, output_path


def extract_uktssa_numbers_for_era_id(df: pd.DataFrame, output_directory: str) -> str:
    """
    Extracts the distinct UKT_UKTSSA_NO column from the export into its own CSV
    for manual editing. The manually edited file is expected to come back
    with two columns: UKT_UKTSSA_NO, ERA_ID.

    Returns the path where the file to be manually edited was written.
    """
    if "UKT_UKTSSA_NO" not in df.columns:
        raise ValueError("Expected column 'UKT_UKTSSA_NO' not found in export.")

    uktssa_df = df[["UKT_UKTSSA_NO"]].drop_duplicates().sort_values("UKT_UKTSSA_NO")
    uktssa_df["ERA_ID"] = ""  # placeholder column for manual entry

    output_path = os.path.join(output_directory, "uktssa_numbers_for_era_id.csv")
    uktssa_df.to_csv(output_path, index=False)
    log.info(f"Wrote {len(uktssa_df)} UKT_UKTSSA_NO row(s) to {output_path} for manual ERA_ID entry.")
    return output_path


def _move_column(df: pd.DataFrame, column: str, position: int) -> pd.DataFrame:
    """Returns a copy of df with `column` moved to the given 0-indexed position."""
    cols = [c for c in df.columns if c != column]
    cols.insert(position, column)
    return df[cols]


def join_era_ids_into_extract(
    df: pd.DataFrame,
    edited_uktssa_path: str,
    output_directory: str,
    version: int,
) -> str:
    """
    Reads the manually edited UKT_UKTSSA_NO/ERA_ID CSV and joins it back into
    the original export on UKT_UKTSSA_NO. Saves the result under a new
    versioned filename.
    """
    era_df = pd.read_csv(edited_uktssa_path, na_filter=False)

    missing_cols = {"UKT_UKTSSA_NO", "ERA_ID"} - set(era_df.columns)
    if missing_cols:
        raise ValueError(f"Edited file is missing expected column(s): {missing_cols}")

    if era_df["ERA_ID"].eq("").any():
        missing_count = era_df["ERA_ID"].eq("").sum()
        log.warning(f"{missing_count} row(s) in {edited_uktssa_path} have a blank ERA_ID.")

    merged_df = df.merge(era_df, on="UKT_UKTSSA_NO", how="left")
    merged_df = _move_column(merged_df, "ERA_ID", 2)  # 3rd column (0-indexed position 2)

    output_path = os.path.join(output_directory, f"ukt_transplant_extract_v{version}.csv")
    merged_df.to_csv(output_path, index=False)
    log.info(f"Joined ERA_ID into extract, saved as {output_path}")
    return output_path


def finalize_extract_with_era_ids(directory: str, extract_version: int = 1) -> str:
    """
    Standalone flow, run separately once uktssa_numbers_for_era_id.csv has
    been manually edited to fill in the ERA_ID column.

    Reads:
      - ukt_transplant_extract_v{extract_version}.csv (the original export
        written by export_ukt_transplant_extract)
      - uktssa_numbers_for_era_id.csv (now filled in with ERA_ID, written by
        extract_uktssa_numbers_for_era_id)

    Joins ERA_ID into the extract on UKT_UKTSSA_NO and saves the result as
    "<extract base name>_final.csv" (e.g. ukt_transplant_extract_final.csv).

    Run standalone via:
        python nhsbt_import.py --finalize
    """
    extract_path = os.path.join(directory, f"ukt_transplant_extract_v{extract_version}.csv")
    if not os.path.exists(extract_path):
        raise FileNotFoundError(f"Extract file not found: {extract_path}")

    edited_uktssa_path = os.path.join(directory, "uktssa_numbers_for_era_id.csv")
    if not os.path.exists(edited_uktssa_path):
        raise FileNotFoundError(f"Edited ERA_ID file not found: {edited_uktssa_path}")

    df = pd.read_csv(extract_path, na_filter=False)
    era_df = pd.read_csv(edited_uktssa_path, na_filter=False)

    missing_cols = {"UKT_UKTSSA_NO", "ERA_ID"} - set(era_df.columns)
    if missing_cols:
        raise ValueError(f"Edited file is missing expected column(s): {missing_cols}")

    if era_df["ERA_ID"].eq("").any():
        missing_count = era_df["ERA_ID"].eq("").sum()
        log.warning(f"{missing_count} row(s) in {edited_uktssa_path} have a blank ERA_ID.")

    merged_df = df.merge(era_df, on="UKT_UKTSSA_NO", how="left")
    merged_df = _move_column(merged_df, "ERA_ID", 2)  # 3rd column (0-indexed position 2)

    base_name = os.path.splitext(os.path.basename(extract_path))[0]  # "ukt_transplant_extract_v1"
    base_name = base_name.rsplit("_v", 1)[0]  # -> "ukt_transplant_extract"
    output_path = os.path.join(directory, f"{base_name.upper()}_{datetime.datetime.now().date()}.csv")
    merged_df.to_csv(output_path, index=False)
    log.info(f"Joined ERA_ID into extract, saved as {output_path}")
    return output_path


def check_unit_names_and_link(directory: str) -> None:
    """
    Standalone post-import step. Run this manually after main() has completed
    and the NHSBT import has been committed:

        python nhsbt_import.py            # runs main()
        python nhsbt_import.py --link      # runs this function

    Steps:
      1. Check UKT_TRANSPLANTS for TRANSPLANT_UNIT names not in UKT_SITES.
      2. Prompt to add any genuine NHS mismatches to UKT_SITES.
      3. Re-run the check to confirm the fix took effect.
      4. Execute PROC_UKT_LINK.
      5. Export VWE_UKT_TRANSPLANT_EXTRACT_NEW to CSV (v1).
      6. Extract UKT_UKTSSA_NO for manual ERA_ID entry.
      7. Join the manually edited ERA_ID data back in, saving as v2.
    """
    session = utils.create_session()
    try:
        unmatched_df = check_unit_names(session)
        df_question=input("pause the program and look at df does it look fine y/n")
        if df_question == "n":
            prompt_and_add_site_codes(session, unmatched_df)

        remaining = check_unit_names(session)
        if not remaining.empty:
            log.warning(
                f"{len(remaining)} unit name(s) still unmatched (expected for "
                f"private/foreign clinics): {remaining['TRANSPLANT_UNIT'].tolist()}"
            )
        else:
            log.info("All TRANSPLANT_UNIT names resolved.")

        run_ukt_link_procedure(session)

        export_df, export_path = export_ukt_transplant_extract(session, directory)
        print(f"Export complete: {export_path}")

        uktssa_path = extract_uktssa_numbers_for_era_id(export_df, directory)
    finally:
        session.close()


if __name__ == "__main__":

    #check_unit_names_and_link(args.directory)
    finalize_extract_with_era_ids(args.directory)