# TODO: Could consider putting these in a table somewhere
df_columns = {
    "match_type_df": [
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
        "DB NHS Number",
    ],
    "patient_field_differences_df": [
        "UKTSSA_No",
        "Field",
        "File Value",
        "Previous Import Value",
    ],
    "transplant_field_differences_df": [
        "UKTSSA_No",
        "Transplant_ID",
        "Field",
        "File Value",
        "Previous Import Value",
    ],
    "invalid_postcode_df": ["UKTSSA_No", "Message", "Value"],
    "invalid_nhs_number_df": ["UKTSSA_No", "Value"],
    "missing_patient_df": ["UKTSSA_No"],
    "missing_transplant_df": ["Transplant_ID"],
}
