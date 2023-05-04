CREATE or replace TABLE country_tbl (
  country_name VARCHAR(100),
  country_code VARCHAR(2),
  currency VARCHAR(50)
);

INSERT INTO country_tbl (country_name, country_code, currency) 
VALUES 
  ('United States', 'US', 'USD'),
  ('Canada', 'CA', 'CAD'),
  ('United Kingdom', 'UK', 'GBP'),
  ('Australia', 'AU', 'AUD'),
  ('Japan', 'JP', 'JPY'),
  ('Switzerland', 'CH', 'CHF'),
  ('Germany', 'DE', 'EUR'),
  ('Brazil', 'BR', 'BRL'),
  ('India', 'IN', 'INR'),
  ('China', 'CN', 'CNY');



CREATE TABLE employee_country_industry (
  employee_name VARCHAR(7),
  role VARCHAR(20),
  country VARCHAR(50),
  industry VARCHAR(20)
);

INSERT INTO employee_country_industry (employee_name, role, country, industry)
;
SELECT 
  e.employee_name,
  e.role,
  c.country_name,
  CASE uniform(0, 4, random())
    WHEN 0 THEN 'Technology'
    WHEN 1 THEN 'Finance'
    WHEN 2 THEN 'Healthcare'
    WHEN 3 THEN 'Manufacturing'
    ELSE 'Retail'
  END
FROM 
  employee_tbl e,
  country_tbl c
ORDER BY RANDom()
LIMIT 10;

CREATE or replace TABLE employee_tbl (
  employee_name VARCHAR(7),
  role VARCHAR(20)
);

INSERT INTO employee_tbl (employee_name, role)
SELECT 
  CONCAT(
    upper(char(uniform(65,90, random()))),
    upper(char(uniform(65,90, random()))),
    uniform(00001, 99999, random())
  ),
  CASE uniform(0, 2, random())
    WHEN 0 THEN 'Manager'
    WHEN 1 THEN 'Developer'
    ELSE 'Designer'
  END
FROM 
  TABLE(GENERATOR(ROWCOUNT => 100));
  
  CREATE TABLE industry_tbl (
  industry_name VARCHAR(20)
);

INSERT INTO industry_tbl (industry_name)
VALUES 
  ('Technology'),
  ('Finance'),
  ('Healthcare'),
  ('Manufacturing'),
  ('Retail'),
  ('Transportation'),
  ('Hospitality'),
  ('Education'),
  ('Energy'),
  ('Media');
CREATE TABLE bank_tbl (
  bank_name VARCHAR(50)
);

INSERT INTO bank_tbl (bank_name)
VALUES 
  ('Bank of America'),
  ('JPMorgan Chase'),
  ('Wells Fargo'),
  ('Citigroup'),
  ('Goldman Sachs'),
  ('Morgan Stanley'),
  ('HSBC'),
  ('Barclays'),
  ('UBS'),
  ('Deutsche Bank');

