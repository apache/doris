CREATE TABLE column_witdh_array (
  street VARCHAR NOT NULL,
  streetaddress VARCHAR NOT NULL,
  k1 JSON   NULL,
  k2 JSON   NULL
) ENGINE=OLAP
DUPLICATE KEY(street, streetaddress)
DISTRIBUTED BY HASH(street) BUCKETS 2
PROPERTIES (
    "replication_num" = "1",
    "disable_auto_compaction" = "true"
)
