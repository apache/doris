-- Reproducer for an Iceberg table whose active snapshot contains both
-- Parquet and ORC data files. Run this file once in the Spark Iceberg container.
-- It deliberately does not drop the table, so rerunning requires a new table name.

CREATE DATABASE IF NOT EXISTS demo.test_db;
USE demo.test_db;

CREATE TABLE mixed_file_format (
    id INT,
    source STRING
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

-- The first snapshot's data files are Parquet.
INSERT INTO mixed_file_format VALUES
    (1, 'parquet'),
    (2, 'parquet'),
    (3, 'parquet');

-- Change only the format for subsequent writes. The Parquet files above remain
-- referenced by the current snapshot, while this append produces ORC files.
ALTER TABLE mixed_file_format
    SET TBLPROPERTIES ('write.format.default' = 'orc');

INSERT INTO mixed_file_format VALUES
    (4, 'orc'),
    (5, 'orc'),
    (6, 'orc');
