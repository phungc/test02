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

