CREATE TABLE timezone
(
    `timestamp` DATETIME
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);
