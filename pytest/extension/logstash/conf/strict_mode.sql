CREATE TABLE strict_mode
(
    `timestamp` DATETIME,
    `id` INT
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);
