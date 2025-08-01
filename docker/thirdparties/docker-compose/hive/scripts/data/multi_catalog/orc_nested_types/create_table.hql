CREATE DATABASE IF NOT EXISTS multi_catalog;
USE multi_catalog;

CREATE TABLE `nested_types1_orc` (
    `id` INT,
    `array_col` ARRAY<INT>,
    `nested_array_col` ARRAY<ARRAY<INT>>,
    `map_col` MAP<STRING, INT>,
    `nested_map_col` MAP<STRING, ARRAY<INT>>,
    `struct_col` STRUCT<`name`: STRING, `age`: INT>,
    `array_struct_col` ARRAY<STRUCT<`name`: STRING, `age`: INT>>,
    `map_struct_col` MAP<STRING, STRUCT<`name`: STRING, `age`: INT>>,
    `complex_struct_col` STRUCT<
        `a`: ARRAY<INT>,
        `b`: MAP<STRING, ARRAY<INT>>,
        `c`: STRUCT<
            `x`: ARRAY<INT>,
            `y`: STRING
        >
    >
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  '/user/doris/suites/multi_catalog/nested_types1_orc';

msck repair table nested_types1_orc;

