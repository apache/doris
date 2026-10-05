drop table if exists employees;
create table employees (
    id INT,
    name VARCHAR(100),
    department VARCHAR(100),
    salary DECIMAL(10,2),
    hire_date DATE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/mnt/scripts/create_preinstalled_scripts/employees.csv' OVERWRITE INTO TABLE employees;
