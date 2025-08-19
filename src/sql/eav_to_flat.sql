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
)
SELECT r.* EXCLUDE(record), m.* EXCLUDE(record)
FROM rest r
  LEFT JOIN multi m ON r.record = m.record
ORDER BY r.record;