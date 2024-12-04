CREATE TABLE columns
(
    `timestamp` DATETIME,
    `id_` INT,
    `msg_` VARCHAR(100)
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);
