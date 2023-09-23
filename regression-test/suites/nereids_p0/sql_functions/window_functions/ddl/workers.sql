CREATE TABLE IF NOT EXISTS tpch_tiny_workers (
  id_employee INT,
  first_name VARCHAR(32),
  last_name VARCHAR(32),
  date_of_employment DATE,
  department TINYINT(1),
  id_department INT,
  name VARCHAR(32),
  salary INT
) DUPLICATE KEY(id_employee) DISTRIBUTED BY HASH(first_name) BUCKETS 3 PROPERTIES ("replication_num" = "1")
