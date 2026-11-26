CREATE TABLE column_witdh_array (
  street VARCHAR NOT NULL,
  streetaddress VARCHAR NOT NULL,
  k1 JSON   NULL,
  k2 JSON   NULL
) ENGINE=OLAP
DUPLICATE KEY(street, streetaddress)
-- 16 buckets keeps each tablet's single loaded rowset around 7GB; with 2 buckets it was ~56GB,
-- and compaction (which writes the full output on the same mount before deleting the input)
-- structurally ENOSPCed on 100GB CI data disks.
DISTRIBUTED BY HASH(street) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "disable_auto_compaction" = "true"
)
