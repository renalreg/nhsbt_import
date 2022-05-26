postcode_query = """
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

patients_query = """
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

identifier_query = """
        SELECT
            RR_NO,
            SURNAME,
            FORENAME,
            SEX,
            DATE_BIRTH,
            NULL,
            NULL,
            CHI_NO,
            NEW_NHS_NO,
            HSC_NO,
            UKTSSA_NO,
            NULL          
        FROM
            PATIENTS
    """

new_identifier_query = """
            SELECT
                A.RR_NO,
                CASE WHEN B.SURNAME IS NULL THEN A.SURNAME ELSE B.SURNAME END AS SURNAME,
                CASE WHEN B.FORENAME IS NULL THEN A.FORENAME ELSE B.FORENAME END AS FORENAME,
                SEX,
                A.DATE_BIRTH,
                NULL,
                NULL,
                CHI_NO,
                NEW_NHS_NO,
                HSC_NO,
                UKTSSA_NO,
                NULL,
                'N' AS DELETED
            FROM
                PATIENTS A				
            LEFT JOIN
                PATIENT_ALIASES B ON A.RR_NO = B.RR_NO

            UNION

            SELECT
                RR_NO,
                SURNAME,
                FORENAME,
                SEX,
                DATE_BIRTH,
                NULL,
                NULL,
                CHI_NO,
                NEW_NHS_NO,
                HSC_NO,
                UKTSSA_NO,
                NULL,
                'Y' AS DELETED
            FROM
                DELETED_PATIENTS A
            """
