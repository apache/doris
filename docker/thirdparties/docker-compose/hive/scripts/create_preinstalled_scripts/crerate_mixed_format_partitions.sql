DROP TABLE IF EXISTS mixed_format_table_hive2;

CREATE TABLE mixed_format_table_hive2 (
    id   INT,
    name STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

ALTER TABLE mixed_format_table_hive2 ADD PARTITION (dt='2023-01-01');
INSERT INTO TABLE mixed_format_table_hive2 PARTITION (dt='2023-01-01') VALUES (1, 'orc_row');

ALTER TABLE mixed_format_table_hive2 ADD PARTITION (dt='2023-01-02');
ALTER TABLE mixed_format_table_hive2 PARTITION (dt='2023-01-02') SET FILEFORMAT PARQUET;
INSERT INTO TABLE mixed_format_table_hive2 PARTITION (dt='2023-01-02') VALUES (2, 'parquet_row');


DROP TABLE IF EXISTS mixed_format_table_hive3;

CREATE TABLE mixed_format_table_hive3 (
    id   INT,
    name STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

ALTER TABLE mixed_format_table_hive3 ADD PARTITION (dt='2023-01-01');
INSERT INTO TABLE mixed_format_table_hive3 PARTITION (dt='2023-01-01') VALUES (1, 'orc_row');

ALTER TABLE mixed_format_table_hive3 ADD PARTITION (dt='2023-01-02');
ALTER TABLE mixed_format_table_hive3 PARTITION (dt='2023-01-02') SET FILEFORMAT PARQUET;
INSERT INTO TABLE mixed_format_table_hive3 PARTITION (dt='2023-01-02') VALUES (2, 'parquet_row');
