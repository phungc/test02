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
  SELECT 
    'United States' AS name,
    'US' AS code
  UNION ALL
  SELECT 
    'Canada' AS name,
    'CA' AS code
  UNION ALL
  SELECT 
    'United Kingdom' AS name,
    'UK' AS code
  UNION ALL
  SELECT 
    'Australia' AS name,
    'AU' AS code
  UNION ALL
  SELECT 
    'Germany' AS name,
    'DE' AS code
) AS country_names
CROSS JOIN (
  SELECT 
    'US' AS code
  UNION ALL
  SELECT 
    'CA' AS code
  UNION ALL
  SELECT 
    'UK' AS code
  UNION ALL
  SELECT 
    'AU' AS code
  UNION ALL
  SELECT 
    'DE' AS code
) AS country_codes;
