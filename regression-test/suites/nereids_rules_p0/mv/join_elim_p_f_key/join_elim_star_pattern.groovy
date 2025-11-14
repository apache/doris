package mv.join_elim_p_f_key
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

suite("join_elim_star_pattern") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"

    sql """
    drop table if exists orders_1
    """

    sql """CREATE TABLE `orders_1` (
      `o_orderkey` BIGINT NOT NULL,
      `o_partkey` INT NOT NULL,
      `o_suppkey` INT NOT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_1
    """

    sql """CREATE TABLE `lineitem_1` (
      `l_orderkey` BIGINT NOT NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NOT NULL,
      `l_suppkey` INT NOT NULL,
      `l_quantity` DECIMAL(15, 2) NULL,
      `l_extendedprice` DECIMAL(15, 2) NULL,
      `l_discount` DECIMAL(15, 2) NULL,
      `l_tax` DECIMAL(15, 2) NULL,
      `l_returnflag` VARCHAR(1) NULL,
      `l_linestatus` VARCHAR(1) NULL,
      `l_commitdate` DATE NULL,
      `l_receiptdate` DATE NULL,
      `l_shipinstruct` VARCHAR(25) NULL,
      `l_shipmode` VARCHAR(10) NULL,
      `l_comment` VARCHAR(44) NULL,
      `l_shipdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists partsupp_1
    """

    sql """CREATE TABLE `partsupp_1` (
      `ps_partkey` INT NOT NULL,
      `ps_suppkey` INT NOT NULL,
      `ps_availqty` INT NULL,
      `ps_supplycost` DECIMAL(15, 2) NULL,
      `ps_comment` VARCHAR(199) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`ps_partkey`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 24
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""


    sql """
    INSERT INTO orders_1 (
        o_orderkey, o_partkey, o_suppkey, o_custkey, o_orderstatus,
        o_totalprice, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderdate
    ) VALUES
    (1001, 501, 201, 3001, 'O', 1500.00, '1-URGENT', 'Clerk#001', 0, '紧急订单', '2024-01-15'),
    (1002, 502, 202, 3002, 'F', 2500.00, '2-HIGH', 'Clerk#002', 1, '普通订单', '2024-02-20'),
    (1003, 503, 203, 3003, 'O', 1800.00, '3-MEDIUM', 'Clerk#003', 0, '中等优先级', '2024-03-05'),
    (1004, 504, 204, 3004, 'F', 3200.00, '4-NOT SPEC', 'Clerk#004', 1, '大额订单', '2024-03-15'),
    (1005, 505, 205, 3005, 'O', 950.00, '5-LOW', 'Clerk#005', 0, '小额订单', '2024-04-01'),
    (1006, 506, 206, 3005, 'O', 950.00, '5-LOW', 'Clerk#005', 0, '小额订单', '2024-04-01'),
    (1007, 507, 207, 3005, 'O', 950.00, '5-LOW', 'Clerk#005', 0, '小额订单', '2024-04-01'),
    (1008, 508, 208, 3005, 'O', 950.00, '5-LOW', 'Clerk#005', 0, '小额订单', '2024-04-01');
    
    INSERT INTO lineitem_1 (
        l_orderkey, l_linenumber, l_partkey, l_suppkey, l_quantity,
        l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
        l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, l_shipdate
    ) VALUES
    (1001, 1, 501, 201, 5.00, 500.00, 0.05, 0.10, 'N', 'O', '2024-01-16', '2024-01-20', 'DELIVER IN PERSON', 'TRUCK', '订单1001第一行', '2024-01-18'),
    (1001, 2, 502, 202, 10.00, 1000.00, 0.10, 0.15, 'N', 'O', '2024-01-16', '2024-01-20', 'NONE', 'AIR', '订单1001第二行', '2024-01-18'),
    (1002, 1, 503, 203, 8.00, 800.00, 0.08, 0.12, 'N', 'O', '2024-02-21', '2024-02-25', 'TAKE BACK RETURN', 'MAIL', '订单1002第一行', '2024-02-22'),
    (1002, 2, 504, 204, 12.00, 1200.00, 0.12, 0.18, 'N', 'O', '2024-02-21', '2024-02-26', 'COLLECT COD', 'SHIP', '订单1002第二行', '2024-02-23'),
    (1003, 1, 505, 205, 6.00, 600.00, 0.06, 0.12, 'N', 'O', '2024-03-06', '2024-03-10', 'DELIVER IN PERSON', 'TRUCK', '订单1003第一行', '2024-03-08'),
    (1003, 2, 506, 206, 9.00, 900.00, 0.09, 0.15, 'N', 'O', '2024-03-06', '2024-03-11', 'NONE', 'AIR', '订单1003第二行', '2024-03-09'),
    (1004, 1, 507, 207, 10.00, 1000.00, 0.10, 0.18, 'N', 'O', '2024-03-16', '2024-03-20', 'TAKE BACK RETURN', 'MAIL', '订单1004第一行', '2024-03-18'),
    (1004, 2, 508, 208, 15.00, 1500.00, 0.15, 0.22, 'N', 'O', '2024-03-16', '2024-03-21', 'COLLECT COD', 'SHIP', '订单1004第二行', '2024-03-19'),
    (1005, 1, 509, 209, 4.00, 400.00, 0.04, 0.08, 'N', 'O', '2024-04-02', '2024-04-06', 'DELIVER IN PERSON', 'TRUCK', '订单1005第一行', '2024-04-04'),
    (1005, 2, 510, 210, 5.50, 550.00, 0.05, 0.11, 'N', 'O', '2024-04-02', '2024-04-07', 'NONE', 'AIR', '订单1005第二行', '2024-04-05');

    INSERT INTO partsupp_1 (
        ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment
    ) VALUES
    (501, 201, 100, 50.00, '零件501供应商201'),
    (502, 202, 200, 75.00, '零件502供应商202'),
    (503, 203, 150, 60.00, '零件503供应商203'),
    (504, 204, 180, 65.00, '零件504供应商204'),
    (505, 205, 120, 55.00, '零件505供应商205'),
    (506, 206, 90, 70.00,  '零件506供应商206'),
    (507, 207, 160, 80.00, '零件507供应商207'),
    (508, 208, 140, 75.00, '零件508供应商208'),
    (509, 209, 110, 85.00, '零件509供应商209'),
    (510, 210, 130, 90.00, '零件510供应商210');
    """

    sql """analyze table lineitem_1 with sync;"""
    sql """analyze table orders_1 with sync;"""
    sql """analyze table partsupp_1 with sync;"""
    sql """alter table lineitem_1 modify column l_comment set stats ('row_count'='10');"""
    sql """alter table orders_1 modify column o_comment set stats ('row_count'='8');"""
    sql """alter table partsupp_1 modify column ps_comment set stats ('row_count'='10');"""


    def compare_res = { def stmt, int orderByColumns = 1 ->
        sql "SET enable_materialized_view_rewrite=false"
        def orderStmt = " order by " + (1..orderByColumns).join(", ")
        def origin_res = sql stmt + orderStmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt + orderStmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def dropAllConstraints = { def tableName ->
        def getConstraintsQuery = "SHOW CONSTRAINTS FROM ${tableName}"
        def constraints = sql getConstraintsQuery
        logger.info("needed deleted constraints : ${constraints}")
        constraints.each { constraint ->
            def constraintName = constraint[0]
            def dropConstraintSQL = "ALTER TABLE ${tableName} DROP CONSTRAINT ${constraintName}"
            sql dropConstraintSQL
            logger.info("delete ${tableName} constraits : ${constraintName}")
        }
    }


    // base mtmv + inner/left join without unique
    def mv_stmt_1 = """
        select l_shipdate, l_linenumber, l_partkey, l_orderkey, o_orderkey, o_partkey, o_suppkey, o_custkey, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost from lineitem_1
        inner join orders_1
        on o_orderkey = l_orderkey
        inner join partsupp_1
        on ps_partkey = l_partkey and ps_suppkey = l_suppkey
        """
    def mv_stmt_2 = """
        select l_shipdate, l_linenumber, l_partkey, l_orderkey, o_orderkey, o_partkey, o_suppkey, o_custkey, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost from lineitem_1
        left join orders_1
        on o_orderkey = l_orderkey
        left join partsupp_1
        on ps_partkey = l_partkey and ps_suppkey = l_suppkey
        """
    
    // single table, use left
    def query_1 = """select l_shipdate, l_linenumber, l_partkey, l_orderkey from lineitem_1"""

    // lineitem_1 join orders_1 use left
    def query_2 = """select l_shipdate, l_linenumber, l_partkey, l_orderkey 
                            from lineitem_1 
                            inner join 
                            orders_1
                            on o_orderkey = l_orderkey"""
    def query_3 = """select l_shipdate, l_linenumber, l_partkey, l_orderkey 
                            from lineitem_1 
                            left join 
                            orders_1
                            on o_orderkey = l_orderkey"""
    // lineitem_1 join orders_1 use both
    def query_4 = """select l_shipdate, l_linenumber, l_partkey, o_orderkey 
                            from lineitem_1 
                            inner join 
                            orders_1
                            on o_orderkey = l_orderkey"""
    def query_5 = """select l_shipdate, l_linenumber, l_partkey, o_orderkey 
                            from lineitem_1 
                            left join 
                            orders_1
                            on o_orderkey = l_orderkey"""
    // lineitem_1 join orders_1 use right
    def query_6 = """select o_orderkey, o_partkey, o_suppkey, o_custkey
                            from lineitem_1 
                            inner join 
                            orders_1
                            on o_orderkey = l_orderkey"""
    def query_7 = """select o_orderkey, o_partkey, o_suppkey, o_custkey
                            from lineitem_1 
                            left join 
                            orders_1
                            on o_orderkey = l_orderkey"""

    // lineitem_1 join partsupp_1 use left
    def query_8 = """select l_shipdate, l_linenumber, l_partkey, l_orderkey 
                             from lineitem_1 
                             inner join 
                             partsupp_1  
                             on ps_partkey = l_partkey and ps_suppkey = l_suppkey"""
    def query_9 = """select l_shipdate, l_linenumber, l_partkey, l_orderkey
                             from lineitem_1 
                             left join partsupp_1  
                             on ps_partkey = l_partkey and ps_suppkey = l_suppkey"""
    // lineitem_1 join partsupp_1 use both
    def query_10 = """select l_shipdate, l_linenumber, l_partkey, ps_partkey
                            from lineitem_1
                            inner join 
                            partsupp_1 on ps_partkey = l_partkey and ps_suppkey = l_suppkey"""
    def query_11 = """select l_shipdate, l_linenumber, l_partkey, ps_partkey
                             from lineitem_1 
                             left join partsupp_1  
                             on ps_partkey = l_partkey and ps_suppkey = l_suppkey"""

    // lineitem_1 join partsupp_1 use right
    def query_12 = """select ps_partkey, ps_suppkey, ps_availqty, ps_supplycost
                            from lineitem_1
                            inner join 
                            partsupp_1 on ps_partkey = l_partkey and ps_suppkey = l_suppkey"""
    def query_13 = """select ps_partkey, ps_suppkey, ps_availqty, ps_supplycost
                             from lineitem_1 
                             left join partsupp_1  
                             on ps_partkey = l_partkey and ps_suppkey = l_suppkey"""


    def query_list = [query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8, query_9,
                      query_10, query_11, query_12, query_13]

    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")

    // lineitem -> orders (fk-pk)
    // lineitem -> partsupp (fk-pk)
    sql """alter table partsupp_1 add constraint pk primary key(ps_partkey, ps_suppkey)"""
    sql """alter table lineitem_1 add constraint fk1 foreign key (l_partkey, l_suppkey) references partsupp_1(ps_partkey, ps_suppkey)"""
    sql """alter table orders_1 add constraint pk primary key(o_orderkey)"""
    sql """alter table lineitem_1 add constraint fk foreign key (l_orderkey) references orders_1(o_orderkey)"""

    def mv_name = "join_elim_star_pattern"
    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""

    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
             mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""

    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // lineitem -> orders (fk-pk)
    sql """alter table orders_1 add constraint pk primary key(o_orderkey)"""
    sql """alter table lineitem_1 add constraint fk foreign key (l_orderkey) references orders_1(o_orderkey)"""
    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in [8, 10, 12]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [9, 11, 13]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // lineitem -> partsupp (fk-pk)
    sql """alter table partsupp_1 add constraint pk primary key(ps_partkey, ps_suppkey)"""
    sql """alter table lineitem_1 add constraint fk1 foreign key (l_partkey, l_suppkey) references partsupp_1(ps_partkey, ps_suppkey)"""

    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in [2, 4, 6]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""

    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [3, 5, 7]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // lineitem -> orders (u)
    // lineitem -> partsupp (fk-pk)
    sql """alter table partsupp_1 add constraint pk primary key (ps_partkey, ps_suppkey)"""
    sql """alter table lineitem_1 add constraint fk1 foreign key (l_partkey, l_suppkey) references partsupp_1(ps_partkey, ps_suppkey)"""
    sql """alter table orders_1 add constraint uk unique (o_orderkey)"""

    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in [2, 4, 6]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // lineitem -> orders (fk-pk)
    // lineitem -> partsupp (u)
    sql """alter table partsupp_1 add constraint pk unique (ps_partkey, ps_suppkey)"""
    sql """alter table orders_1 add constraint pk primary key (o_orderkey)"""
    sql """alter table lineitem_1 add constraint fk foreign key (l_orderkey) references orders_1(o_orderkey)"""

    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in [8, 10, 12]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [1, 2, 3, 4, 5, 6, 7, 9, 11, 13]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // lineitem -> orders (u)
    // lineitem -> partsupp (u)
    sql """alter table partsupp_1 add constraint pk unique (ps_partkey, ps_suppkey)"""
    sql """alter table orders_1 add constraint pk unique (o_orderkey)"""

    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in []) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [1, 2, 3, 4, 5, 6, 7, 9, 11, 13]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // lineitem -> orders (u)
    sql """alter table orders_1 add constraint pk unique (o_orderkey)"""
    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in []) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""

    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [9, 11, 13]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // lineitem -> partsupp (u)
    sql """alter table partsupp_1 add constraint pk unique (ps_partkey, ps_suppkey)"""

    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in []) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""

    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in [3, 5, 7]) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")
    // without constraints
    create_async_mv(db, mv_name, mv_stmt_1)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("inner mv current query index: " + j)
        if (j in []) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""

    create_async_mv(db, mv_name, mv_stmt_2)
    for (int j = 1; j < query_list.size() + 1; j++) {
        logger.info("left mv current query index: " + j)
        if (j in []) {
            mv_rewrite_success(query_list[j - 1], mv_name)
            compare_res(query_list[j - 1], 4)
        } else {
            mv_rewrite_fail(query_list[j - 1], mv_name)
        }
    }
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""


    dropAllConstraints("orders_1")
    dropAllConstraints("lineitem_1")
    dropAllConstraints("partsupp_1")

    // negative examplesorders_1, lineitem_1, partsupp_1
    sql """alter table partsupp_1 add constraint pk unique (ps_partkey, ps_suppkey)"""
    async_mv_rewrite_fail(db,
            """
             select l_shipdate, l_linenumber, o_orderkey, ps_suppkey from lineitem_1
             inner join orders_1
             on o_orderkey = l_orderkey
             inner join partsupp_1
             on ps_partkey = l_partkey or ps_suppkey = l_suppkey
            """,
            """
            select l_shipdate, l_linenumber, o_orderkey
            from lineitem_1
            inner join orders_1 on o_orderkey = l_orderkey
            """,
            "${mv_name}_neg_2")
    sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}_neg_1;"""

    sql """
    drop table if exists orders_1;
    drop table if exists lineitem_1;
    drop table if exists partsupp_1;
    """
}
