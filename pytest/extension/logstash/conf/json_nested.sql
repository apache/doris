CREATE TABLE json_nested
(
    `timestamp` DATETIME,
    `id` INT,
    `msg_str` STRING,
    `msg_json` JSON,
    `msg_variant` VARIANT
) ENGINE = OLAP
DUPLICATE KEY(`timestamp`)
PROPERTIES (
"replication_num" = "1"
);
