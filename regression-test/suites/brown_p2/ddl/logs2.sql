CREATE TABLE IF NOT EXISTS logs2 (
 `log_time`    DATETIME NOT NULL,
 `client_ip`   VARCHAR(128),
 `request`     STRING,
 `status_code` SMALLINT,
 `object_size` BIGINT
)
ENGINE = olap
DUPLICATE KEY(`log_time`)
DISTRIBUTED BY HASH(client_ip) BUCKETS 10
PROPERTIES("replication_num" = "1");
