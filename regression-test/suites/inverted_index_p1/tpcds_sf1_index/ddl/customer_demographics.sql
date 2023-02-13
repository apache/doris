CREATE TABLE IF NOT EXISTS customer_demographics (
    cd_demo_sk bigint,
    cd_gender char(1),
    cd_marital_status char(1),
    cd_education_status char(20),
    cd_purchase_estimate integer,
    cd_credit_rating char(10),
    cd_dep_count integer,
    cd_dep_employed_count integer,
    cd_dep_college_count integer,
    INDEX cd_demo_sk_idx(cd_demo_sk) USING INVERTED COMMENT "cd_demo_sk index",
    INDEX cd_gender_idx(cd_gender) USING INVERTED COMMENT "cd_gender index",
    INDEX cd_education_status_idx(cd_education_status) USING INVERTED PROPERTIES("parser"="english") COMMENT "cd_education_status index"
)
DUPLICATE KEY(cd_demo_sk, cd_gender)
DISTRIBUTED BY HASH(cd_gender) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
)
