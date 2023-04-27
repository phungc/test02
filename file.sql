CREATE TABLE country_tbl (
  country_id INTEGER PRIMARY KEY AUTOINCREMENT,
  country_name VARCHAR(50) UNIQUE NOT NULL,
  country_code VARCHAR(2) UNIQUE NOT NULL,
  currency VARCHAR(3) NOT NULL
);

INSERT INTO country_tbl (country_name, country_code, currency) VALUES
  ('United States', 'US', 'USD'),
  ('Canada', 'CA', 'CAD'),
  ('Mexico', 'MX', 'MXN'),
  ('United Arab Emirates', 'AE', 'AED'),
  ('India', 'IN', 'INR'),
  ('Japan', 'JP', 'JPY'),
  ('Australia', 'AU', 'AUD');
