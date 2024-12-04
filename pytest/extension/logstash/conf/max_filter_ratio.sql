CREATE TABLE max_filter_ratio
(
    `timestamp` DATETIME,
    `id` INT
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);
