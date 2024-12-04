CREATE TABLE csv_basic
(   
    `timestamp` DATETIME,
    `id` INT,
    `msg` VARCHAR(100)
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);