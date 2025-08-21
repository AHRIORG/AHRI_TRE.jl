WITH
src AS (
  FROM $1
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
SELECT * FROM pivoted;

