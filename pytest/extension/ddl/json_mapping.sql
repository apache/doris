CREATE TABLE json_mapping
(
    `timestamp` DATETIME,
    `msg` VARCHAR(100),
    `type` VARCHAR(100)
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);
