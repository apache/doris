create database if not exists demo.test_timestamp_tz;
USE demo.test_timestamp_tz;

DROP TABLE IF EXISTS test_ice_timestamp_tz_orc;
DROP TABLE IF EXISTS test_ice_timestamp_tz_parquet;

SET spark.sql.session.timeZone = Asia/Shanghai;

CREATE TABLE test_ice_timestamp_tz_orc (
    id INT,
    ts_tz TIMESTAMP_LTZ
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'orc',
    'format-version' = '1'
);

CREATE TABLE test_ice_timestamp_tz_parquet (
    id INT,
    ts_tz TIMESTAMP_LTZ
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '1'
);

INSERT INTO test_ice_timestamp_tz_orc VALUES (1, TIMESTAMP_LTZ '2025-01-01 00:00:00');
INSERT INTO test_ice_timestamp_tz_orc VALUES (2, TIMESTAMP_LTZ '2025-06-01 12:34:56.789');
INSERT INTO test_ice_timestamp_tz_orc VALUES (3, TIMESTAMP_LTZ '2025-12-31 23:59:59.999999');
INSERT INTO test_ice_timestamp_tz_orc VALUES (4, NULL);


INSERT INTO test_ice_timestamp_tz_parquet VALUES (1, TIMESTAMP_LTZ '2025-01-01 00:00:00');
INSERT INTO test_ice_timestamp_tz_parquet VALUES (2, TIMESTAMP_LTZ '2025-06-01 12:34:56.789');
INSERT INTO test_ice_timestamp_tz_parquet VALUES (3, TIMESTAMP_LTZ '2025-12-31 23:59:59.999999');
INSERT INTO test_ice_timestamp_tz_parquet VALUES (4, NULL);

DROP TABLE IF EXISTS test_ice_timestamp_tz_orc_write_with_mapping;
DROP TABLE IF EXISTS test_ice_timestamp_tz_parquet_write_with_mapping;

CREATE TABLE test_ice_timestamp_tz_orc_write_with_mapping (
    id INT,
    ts_tz TIMESTAMP_LTZ
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'orc',
    'format-version' = '1'
);

CREATE TABLE test_ice_timestamp_tz_parquet_write_with_mapping (
    id INT,
    ts_tz TIMESTAMP_LTZ
)
USING iceberg
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '1'
);
