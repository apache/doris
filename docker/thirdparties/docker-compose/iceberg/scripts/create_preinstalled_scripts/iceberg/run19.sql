create database if not exists demo.test_db;
use demo.test_db;


CREATE TABLE test_invalid_avro_name_parquet (
    id INT,
    `TEST:A1B2.RAW.ABC-GG-1-A` STRING
)
USING iceberg
TBLPROPERTIES(
  'write.format.default' = 'parquet'
);


CREATE TABLE test_invalid_avro_name_orc (
    id INT,
    `TEST:A1B2.RAW.ABC-GG-1-A` STRING
)
USING iceberg
TBLPROPERTIES(
  'write.format.default' = 'orc'
);

INSERT INTO test_invalid_avro_name_parquet VALUES
    (1, 'row1'),
    (2, 'row2'),
    (3, 'row3'),
    (4, 'row4'),
    (5, 'row5');

INSERT INTO test_invalid_avro_name_orc VALUES
    (1, 'row1'),
    (2, 'row2'),
    (3, 'row3'),
    (4, 'row4'),
    (5, 'row5');


