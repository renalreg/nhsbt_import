from datetime import datetime
from typing import Optional
import pandas as pd


class ExtractedRow:
    def __init__(self, row):
        self.renalregno = self._process_number_value(row, "rrno")
        self.surname = self._process_value(row, "surname")
        self.forename = self._process_value(row, "forename")
        self.sex = self._process_sex_value(row, "sex")
        self.dob = self._process_date_value(row, "dob")
        self.dod = self._process_date_value(row, "dod")
        self.bapnno = self._process_value(row, "bapnno")
        self.chi_no = self._process_number_value(row, "chino")
        self.nhs_no = self._process_number_value(row, "nhsno")
        self.hsc_no = self._process_number_value(row, "hscno")

    def _process_value(self, row, identifier) -> Optional[str]:
        if identifier in row:
            return row[identifier] if pd.notna(row[identifier]) else None
        return None

    def _process_number_value(self, row, identifier) -> Optional[int]:
        if identifier in row:
            # cast to int required because pandas produces floats, see
            # https://pandas.pydata.org/pandas-docs/stable/user_guide/gotchas.html#support-for-integer-na
            return int(row[identifier]) if pd.notna(row[identifier]) else None
        return None

    def _process_date_value(self, row, identifier) -> Optional[datetime]:
        if identifier in row:
            return (
                datetime.strptime(row[identifier], "%d/%m/%Y")
                if pd.notna(row[identifier])
                else None
            )
        return None

    def _process_sex_value(self, row, identifier) -> Optional[int]:
        if identifier in row:
            sex = row[identifier][0].lower()
            if sex == "male":
                return 1
            elif sex == "female":
                return 2
            else:
                return None
        return None

    # (rr_no, surname, forename, sex, dob, bapn_no, chi_no, nhs_no, hsc_no)
    def __iter__(self):
        yield self.rrno
        yield self.surname
        yield self.forename
        yield self.sex
        yield self.dob
        yield self.bapnno
        yield self.chi_no
        yield self.nhs_no
        yield self.hsc_no
