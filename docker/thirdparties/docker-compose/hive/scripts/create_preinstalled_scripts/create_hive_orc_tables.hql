create database if not exists multi_catalog;
use multi_catalog;

CREATE TABLE complex_data_orc (
  id INT,
  m MAP<STRING, INT>,
  l ARRAY<STRING>
)
STORED AS ORC
LOCATION
  '/user/doris/preinstalled_data/orc/complex_data_orc';
