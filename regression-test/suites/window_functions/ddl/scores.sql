CREATE TABLE IF NOT EXISTS tpch_tiny_scores
(
    s_id varchar,
    s_grade string,
    s_class string,
    s_math_score int,
    s_english_score int
) ENGINE=OLAP
UNIQUE KEY(`s_id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_id`) BUCKETS 4
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
);