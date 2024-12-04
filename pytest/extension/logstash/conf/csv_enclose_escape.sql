CREATE TABLE csv_enclose_escape
(   
    `timestamp` DATETIME,
    `id` INT,
    `msg` VARCHAR(100)
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);