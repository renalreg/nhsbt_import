from datetime import datetime
from typing import Optional, List, Type

from rr_ukt_import.extracted_row import ExtractedRow
from rr_ukt_import.utils import load_file, clean_headers


def extract_file(file: str, death_threshold: datetime) -> List[Type[ExtractedRow]]:
    """
    Load a file normalize the headers and build a list of extracted row objects

    Args:
        file (str): _description_
        death_threshold (datetime): _description_

    Returns:
        ExtractedRow: an object containing the attributes of the row
    """

    paeds_df = load_file(file)
    paeds_df.columns = clean_headers(paeds_df)
    extracted_rows: List[Type[ExtractedRow]] = []

    for _, row in paeds_df.iterrows():
        extract_row: Type[ExtractedRow] = ExtractedRow(row)
        # Filter deceased patients
        if (
            not extract_row.dod
            or extract_row.dod >= death_threshold
            and any((extract_row.nhs_no, extract_row.chi_no, extract_row.hsc_no))
        ):
            # If column names don't match this will return all Nones for that column
            extracted_rows.append(extract_row)

    return extracted_rows


def process(
    file_path: Optional[str] = None,
    death_threshold: datetime = datetime(1900, 1, 1),
) -> Optional[List[Type[ExtractedRow]]]:

    return extract_file(file_path, death_threshold) if file_path else None
