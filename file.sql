CREATE OR REPLACE TABLE country_tbl (
  country_id INTEGER,
  country_name VARCHAR(50),
  country_code CHAR(2)
);

INSERT INTO country_tbl (
  country_id, country_name, country_code
)
SELECT 
  ROW_NUMBER() OVER (ORDER BY SEQ4()) AS country_id,
  country_names.name AS country_name,
  country_codes.code AS country_code
FROM 
  TABLE(GENERATOR(ROWCOUNT => 100)) AS d
CROSS JOIN (
  SELECT DISTINCT
    'United States' AS name,
    'US' AS code
  UNION ALL
  SELECT DISTINCT
    'Canada' AS name,
    'CA' AS code
  UNION ALL
  SELECT DISTINCT
    'United Kingdom' AS name,
    'UK' AS code
  UNION ALL
  SELECT DISTINCT
    'Australia' AS name,
    'AU' AS code
  UNION ALL
  SELECT DISTINCT
    'Germany' AS name,
    'DE' AS code
) AS country_names
CROSS JOIN (
  SELECT DISTINCT
    'US' AS code
  UNION ALL
  SELECT DISTINCT
    'CA' AS code
  UNION ALL
  SELECT DISTINCT
    'UK' AS code
  UNION ALL
  SELECT DISTINCT
    'AU' AS code
  UNION ALL
  SELECT DISTINCT
    'DE' AS code
) AS country_codes;
