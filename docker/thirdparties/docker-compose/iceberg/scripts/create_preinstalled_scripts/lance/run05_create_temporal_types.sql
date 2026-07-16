-- Spark timestamps are written with the session time zone configured in spark-defaults.conf.
DROP TABLE IF EXISTS lance.doris.temporal_types;

CREATE TABLE lance.doris.temporal_types (
    row_id BIGINT NOT NULL,
    date_value DATE,
    timestamp_value TIMESTAMP
) USING lance;

INSERT INTO lance.doris.temporal_types VALUES
    (1, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.123456'),
    (2, DATE '1970-01-01', TIMESTAMP '1970-01-01 08:00:00.000001');

INSERT INTO lance.doris.temporal_types VALUES
    (3, DATE '2024-02-29', TIMESTAMP '2024-02-29 12:34:56.654321'),
    (4, NULL, NULL);
