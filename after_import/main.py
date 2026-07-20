import os
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


def export_ukt_transplant_extract(session, output_directory: str) -> str:
    """
    Queries VWE_UKT_TRANSPLANT_EXTRACT_NEW and writes the results to CSV,
    replacing the manual SSMS 'copy with headers -> Excel -> Save As CSV' step.
    """
    result = session.execute(text("SELECT * FROM VWE_UKT_TRANSPLANT_EXTRACT_NEW"))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())

    output_path = os.path.join(output_directory, "ukt_transplant_extract.csv")
    df.to_csv(output_path, index=False)
    log.info(f"Exported {len(df)} row(s) to {output_path}")
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
      5. Export VWE_UKT_TRANSPLANT_EXTRACT_NEW to CSV.
    """
    session = utils.create_session()
    try:
        unmatched_df = check_unit_names(session)
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
        export_path = export_ukt_transplant_extract(session, directory)
        print(f"Export complete: {export_path}")
    finally:
        session.close()


if __name__ == "__main__":
    check_unit_names_and_link(args.directory)