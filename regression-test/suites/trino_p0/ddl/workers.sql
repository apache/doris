CREATE TABLE IF NOT EXISTS `workers` (
  id_employee INT,
  first_name VARCHAR(32),
  last_name VARCHAR(32),
  date_of_employment DATE,
  department TINYINT(1),
  id_department INT,
  name VARCHAR(32),
  salary INT
)
DISTRIBUTED BY HASH(`id_employee`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
