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
