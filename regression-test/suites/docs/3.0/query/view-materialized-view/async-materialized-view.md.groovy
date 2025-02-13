// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.junit.jupiter.api.Assertions;

suite("docs/3.0/query/view-materialized-view/async-materialized-view.md") {
    try {
        multi_sql """
        create database tpch;
        create database demo;
        use tpch;
        
        CREATE TABLE IF NOT EXISTS tpch.orders  (
            o_orderkey       integer not null,
            o_custkey        integer not null,
            o_orderstatus    char(1) not null,
            o_totalprice     decimalv3(15,2) not null,
            o_orderdate      date not null,
            o_orderpriority  char(15) not null,
            o_clerk          char(15) not null,
            o_shippriority   integer not null,
            o_comment        varchar(79) not null
            )
            DUPLICATE KEY(o_orderkey, o_custkey)
            PARTITION BY RANGE(o_orderdate)(
            FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
            DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3;
        
        
        insert into tpch.orders values
           (1, 1, 'o', 99.5, '2023-10-17', 'a', 'b', 1, 'yy'),
           (2, 2, 'o', 109.2, '2023-10-18', 'c','d',2, 'mm'),
           (3, 3, 'o', 99.5, '2023-10-19', 'a', 'b', 1, 'yy');
        
        
        CREATE TABLE IF NOT EXISTS tpch.lineitem (
            l_orderkey    integer not null,
            l_partkey     integer not null,
            l_suppkey     integer not null,
            l_linenumber  integer not null,
            l_quantity    decimalv3(15,2) not null,
            l_extendedprice  decimalv3(15,2) not null,
            l_discount    decimalv3(15,2) not null,
            l_tax         decimalv3(15,2) not null,
            l_returnflag  char(1) not null,
            l_linestatus  char(1) not null,
            l_shipdate    date not null,
            l_commitdate  date not null,
            l_receiptdate date not null,
            l_shipinstruct char(25) not null,
            l_shipmode     char(10) not null,
            l_comment      varchar(44) not null
            )
            DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
            PARTITION BY RANGE(l_shipdate)
            (FROM ('2023-10-17') TO ('2023-10-20') INTERVAL 1 DAY)
            DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3;
        
        insert into tpch.lineitem values
         (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
         (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
         (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
        
        
         CREATE MATERIALIZED VIEW tpch.mv1 
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(l_shipdate)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1') 
                AS 
                select l_shipdate, o_orderdate, l_partkey, l_suppkey, sum(o_totalprice) as sum_total
                    from tpch.lineitem
                    left join tpch.orders on lineitem.l_orderkey = orders.o_orderkey and l_shipdate = o_orderdate
                    group by
                    l_shipdate,
                    o_orderdate,
                    l_partkey,
                    l_suppkey;
        
        select * from mv_infos("database"="tpch") where Name="mv1";

        """

        multi_sql """
        SHOW PARTITIONS FROM tpch.mv1;
        REFRESH MATERIALIZED VIEW tpch.mv1 partitions(p_20231017_20231018);
        select * from jobs("type"="mv") order by CreateTime;
        PAUSE MATERIALIZED VIEW JOB ON tpch.mv1;
        RESUME MATERIALIZED VIEW JOB ON tpch.mv1;
        select * from tasks("type"="mv");
        CANCEL MATERIALIZED VIEW TASK realTaskId on tpch.mv1;
        ALTER MATERIALIZED VIEW tpch.mv1 set("grace_period"="3333");
        DROP MATERIALIZED VIEW tpch.mv1;
        """


        multi_sql """
        CREATE TABLE tpch.t1 (
          user_id LARGEINT NOT NULL,
          o_date DATE NOT NULL,
          num SMALLINT NOT NULL
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        PARTITION BY RANGE(o_date)
        (
        PARTITION p20170101 VALUES [('2017-01-01'), ('2017-01-02')),
        PARTITION p20170102 VALUES [('2017-01-02'), ('2017-01-03')),
        PARTITION p20170201 VALUES [('2017-02-01'), ('2017-02-02'))
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2;
        
        
        CREATE TABLE tpch.t2 (
          user_id LARGEINT NOT NULL,
          age SMALLINT NOT NULL
        ) ENGINE=OLAP
        PARTITION BY LIST(age)
        (
            PARTITION p1 VALUES IN ('1'),
            PARTITION p2 VALUES IN ('2')
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2;
        
        
        
        CREATE MATERIALIZED VIEW tpch.mv1
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`order_date`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT t1.o_date as order_date, t1.user_id as user_id, t1.num, t2.age FROM t1 join t2 on t1.user_id=t2.user_id;
        
        
        CREATE MATERIALIZED VIEW tpch.mv2
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`age`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT t1.o_date as order_date, t1.user_id as user_id, t1.num, t2.age FROM tpch.t1 join tpch.t2 on t1.user_id=t2.user_id;
        """



        multi_sql """
        CREATE TABLE demo.t1 (
            `k1` INT,
            `k2` DATE NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY range(`k2`)
        (
        PARTITION p26 VALUES [("2024-03-26"),("2024-03-27")),
        PARTITION p27 VALUES [("2024-03-27"),("2024-03-28")),
        PARTITION p28 VALUES [("2024-03-28"),("2024-03-29"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2;
        
        
        CREATE MATERIALIZED VIEW demo.mv1
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`k2`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'partition_sync_limit'='1',
        'partition_sync_time_unit'='DAY'
        )
        AS
        SELECT * FROM demo.t1;

        CREATE TABLE demo.`t1` (
          `k1` LARGEINT NOT NULL,
          `k2` DATE NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY range(`k2`)
        (
        PARTITION p_20200101 VALUES [("2020-01-01"),("2020-01-02")),
        PARTITION p_20200102 VALUES [("2020-01-02"),("2020-01-03")),
        PARTITION p_20200201 VALUES [("2020-02-01"),("2020-02-02"))
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;

        CREATE MATERIALIZED VIEW demo.mv1
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by (date_trunc(`k2`,'month'))
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * FROM demo.t1;



        CREATE MATERIALIZED VIEW demo.mv1
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by (date_trunc(`k2`,'year'))
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * FROM demo.t1;
        """


    } catch (Throwable t) {
        Assertions.fail("examples in docs/3.0/query/view-materialized-view/async-materialized-view.md failed to exec, please fix it", t)
    }
}
