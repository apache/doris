CREATE TABLE IF NOT EXISTS customer_demographics (
    cd_demo_sk bigint,
    cd_gender char(1),
    cd_marital_status char(1),
    cd_education_status char(20),
    cd_purchase_estimate integer,
    cd_credit_rating char(10),
    cd_dep_count integer,
    cd_dep_employed_count integer,
    cd_dep_college_count integer
)
UNIQUE KEY(cd_demo_sk)
CLUSTER BY(cd_dep_count)
DISTRIBUTED BY HASH(cd_demo_sk) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
)
