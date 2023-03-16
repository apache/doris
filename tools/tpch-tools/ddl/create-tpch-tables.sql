-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
-- 
--   http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

drop table if exists lineitem;
CREATE TABLE lineitem (
    l_shipdate    DATEV2 NOT NULL,
    l_orderkey    bigint NOT NULL,
    l_linenumber  int not null,
    l_partkey     int NOT NULL,
    l_suppkey     int not null,
    l_quantity    decimalv3(15, 2) NOT NULL,
    l_extendedprice  decimalv3(15, 2) NOT NULL,
    l_discount    decimalv3(15, 2) NOT NULL,
    l_tax         decimalv3(15, 2) NOT NULL,
    l_returnflag  VARCHAR(1) NOT NULL,
    l_linestatus  VARCHAR(1) NOT NULL,
    l_commitdate  DATEV2 NOT NULL,
    l_receiptdate DATEV2 NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode     VARCHAR(10) NOT NULL,
    l_comment      VARCHAR(44) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`l_shipdate`, `l_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "lineitem_orders"
);

drop table if exists orders;
CREATE TABLE orders  (
    o_orderkey       bigint NOT NULL,
    o_orderdate      DATEV2 NOT NULL,
    o_custkey        int NOT NULL,
    o_orderstatus    VARCHAR(1) NOT NULL,
    o_totalprice     decimalv3(15, 2) NOT NULL,
    o_orderpriority  VARCHAR(15) NOT NULL,
    o_clerk          VARCHAR(15) NOT NULL,
    o_shippriority   int NOT NULL,
    o_comment        VARCHAR(79) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`o_orderkey`, `o_orderdate`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "lineitem_orders"
);

drop table if exists partsupp;
CREATE TABLE partsupp (
    ps_partkey          int NOT NULL,
    ps_suppkey     int NOT NULL,
    ps_availqty    int NOT NULL,
    ps_supplycost  decimalv3(15, 2)  NOT NULL,
    ps_comment     VARCHAR(199) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`ps_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "part_partsupp"
);

drop table if exists part;
CREATE TABLE part (
    p_partkey          int NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        VARCHAR(25) NOT NULL,
    p_brand       VARCHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        int NOT NULL,
    p_container   VARCHAR(10) NOT NULL,
    p_retailprice decimalv3(15, 2) NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "part_partsupp"
);

drop table if exists customer;
CREATE TABLE customer (
    c_custkey     int NOT NULL,
    c_name        VARCHAR(25) NOT NULL,
    c_address     VARCHAR(40) NOT NULL,
    c_nationkey   int NOT NULL,
    c_phone       VARCHAR(15) NOT NULL,
    c_acctbal     decimalv3(15, 2)   NOT NULL,
    c_mktsegment  VARCHAR(10) NOT NULL,
    c_comment     VARCHAR(117) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24
PROPERTIES (
    "replication_num" = "1"
);

drop table if exists supplier;
CREATE TABLE supplier (
    s_suppkey       int NOT NULL,
    s_name        VARCHAR(25) NOT NULL,
    s_address     VARCHAR(40) NOT NULL,
    s_nationkey   int NOT NULL,
    s_phone       VARCHAR(15) NOT NULL,
    s_acctbal     decimalv3(15, 2) NOT NULL,
    s_comment     VARCHAR(101) NOT NULL
)ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
PROPERTIES (
    "replication_num" = "1"
);

drop table if exists nation;
CREATE TABLE `nation` (
  `n_nationkey` int(11) NOT NULL,
  `n_name`      varchar(25) NOT NULL,
  `n_regionkey` int(11) NOT NULL,
  `n_comment`   varchar(152) NULL
) ENGINE=OLAP
DUPLICATE KEY(`N_NATIONKEY`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

drop table if exists region;
CREATE TABLE region  (
    r_regionkey      int NOT NULL,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152)
)ENGINE=OLAP
DUPLICATE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

drop view if exists revenue0;
create view revenue0 (supplier_no, total_revenue) as
select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
from
    lineitem
where
    l_shipdate >= date '1996-01-01'
    and l_shipdate < date '1996-01-01' + interval '3' month
group by
    l_suppkey;
