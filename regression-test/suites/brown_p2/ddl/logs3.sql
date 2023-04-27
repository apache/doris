CREATE TABLE IF NOT EXISTS logs3 (
 `log_time`     DATETIME NOT NULL,
 `event_type`   VARCHAR(128),
 `device_id`    CHAR(15),
 `device_name`  STRING,
 `device_type`  STRING,
 `device_floor` TINYINT,
 `event_unit`   CHAR(1),
 `event_value`  FLOAT
)
ENGINE = olap
DUPLICATE KEY(`log_time`, `event_type`)
DISTRIBUTED BY HASH(device_id) BUCKETS 10
PROPERTIES("replication_num" = "1");
