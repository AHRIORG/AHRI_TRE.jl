--
WITH multi AS (
	SELECT 
	  record,
	  string_agg(c.value, ', ') AS cs_coh_countries
	FROM '/Users/kobush/Data/datalake/ingests/redcap_records_20250818_064058_0e076a06-5537-468e-9ca1-d50806c19751.csv.zst' c
	WHERE field_name IN ('cs_coh_countries')
	GROUP BY record
),
rest AS (
	PIVOT (FROM '/Users/kobush/Data/datalake/ingests/redcap_records_20250818_064058_0e076a06-5537-468e-9ca1-d50806c19751.csv.zst'
	       WHERE NOT field_name IN ('cs_coh_countries'))
	ON field_name
	USING first("value")
	GROUP BY record
),
flat AS (
SELECT r.* EXCLUDE(record), m.* EXCLUDE(record)
FROM rest r
  LEFT JOIN multi m ON r.record = m.record
)
SELECT 
  CAST(record_id AS INTEGER) record_id,
  CAST(cs_cohort_id AS INTEGER) cs_cohort_id
FROM flat
ORDER BY record_id;
--
CREATE SCHEMA tre_lake;
--
CREATE OR REPLACE TABLE tre_lake."redcap_1194" AS
WITH src AS (
    SELECT * FROM read_csv_auto('/Users/kobush/Data/datalake/ingests/redcap_records_20250818_064058_0e076a06-5537-468e-9ca1-d50806c19751.csv.zst')
),
counts AS (
    SELECT record, field_name, COUNT(*) AS n
    FROM src
    GROUP BY record, field_name
),
multi_fields AS (
    SELECT DISTINCT field_name
    FROM counts
    WHERE n > 1
),
single_src AS (
    SELECT s.*
    FROM src s
    LEFT JOIN multi_fields m USING (field_name)
    WHERE m.field_name IS NULL
),
multi_src AS (
    SELECT record, field_name, string_agg("value", ', ') AS value
    FROM src
    WHERE field_name IN (SELECT field_name FROM multi_fields)
    GROUP BY record, field_name
),
prepped AS (
    SELECT record, field_name, value FROM single_src
    UNION ALL
    SELECT record, field_name, value FROM multi_src
),
pivoted AS (
    PIVOT prepped
    ON field_name
    USING any_value(value)   -- or first(value)
    GROUP BY record
)
SELECT * FROM pivoted
ORDER BY record;
--
-- CREATE OR REPLACE TABLE tre_lake."redcap_1194" AS
SELECT *
FROM (
    WITH src AS (
        SELECT * FROM read_csv_auto('/Users/kobush/Data/datalake/ingests/redcap_records_20250818_064058_0e076a06-5537-468e-9ca1-d50806c19751.csv.zst')
    ),
    counts AS (
        SELECT record, field_name, COUNT(*) AS n
        FROM src
        GROUP BY record, field_name
    ),
    multi_fields AS (
        SELECT DISTINCT field_name
        FROM counts
        WHERE n > 1
    ),
    single_src AS (
        SELECT s.*
        FROM src s
        LEFT JOIN multi_fields m USING (field_name)
        WHERE m.field_name IS NULL
    ),
    multi_src AS (
        SELECT record, field_name, string_agg("value", ', ') AS value
        FROM src
        WHERE field_name IN (SELECT field_name FROM multi_fields)
        GROUP BY record, field_name
    ),
    prepped AS (
        SELECT record, field_name, value FROM single_src
        UNION ALL
        SELECT record, field_name, value FROM multi_src
    ),
    pivoted AS (
        PIVOT prepped
        ON field_name
        USING any_value(value)
        GROUP BY record
    )
    SELECT
	  CAST(record_id AS INTEGER) record_id,
	  CAST(cs_cohort_id AS INTEGER) cs_cohort_id,
	  CAST(cs_cohort_active AS INTEGER) cs_cohort_active,
	  CAST(cs_cohort_size AS INTEGER) cs_cohort_size,
	  CAST(cs_cohort_type AS INTEGER) cs_cohort_type,
	  CAST(cohort_survey_complete AS INTEGER) cohort_survey_complete,
      * EXCLUDE(record,record_id,cs_cohort_id,cs_cohort_active,cs_cohort_size, cs_cohort_type,cohort_survey_complete)
    FROM pivoted
    ORDER BY record_id
) t;
--
SELECT * FROM '/Users/kobush/Library/CloudStorage/OneDrive-AHRI/ACDIS/Research Operations/Population Intervention/HDSS 25yr/outputs/enriched_with_citations_with_IF_UPDATED_20250922_071759.csv';