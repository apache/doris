
CREATE TABLE IF NOT EXISTS nation (
    nationkey  BIGINT NOT NULL,
    name       VARCHAR(25) NOT NULL,
    regionkey  BIGINT NOT NULL,
    comment    VARCHAR(152)
)
DUPLICATE KEY(nationkey)
DISTRIBUTED BY HASH(nationkey) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
)