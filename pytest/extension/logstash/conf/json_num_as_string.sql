CREATE TABLE json_num_as_string
(
    `timestamp` DATETIME,
    `id` STRING,
    `f` STRING
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);
