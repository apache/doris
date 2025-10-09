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

// Test the case where a constant appears in the select list position of an SQL statement containing the JOIN operator.
// eg: select plac_1 from tb left join (select plac_1 as col1 from tb) as t1 on plac_1 = t1.col1
suite ("constant_genaration_random_mtmv_6", "constant_genaration_random_mtmv") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    String table1 = "lineitem_constant_genaration_6"
    String table2 = "orders_constant_genaration_6"
    String mtmv_name = "constant_genaration_mtmv_6"

    sql "set disable_nereids_rules='CONSTANT_PROPAGATION'"

    sql """
    drop table if exists ${table2}
    """

    sql """CREATE TABLE `${table2}` (
      `o_orderkey` BIGINT NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`o_orderdate`, 'day')) ()
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists ${table1}
    """

    sql """CREATE TABLE `${table1}` (
      `l_orderkey` BIGINT NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_suppkey` INT NULL,
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
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${table2} values 
    (null, 1, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (0, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (1, null, 'o', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'k', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'o', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into ${table1} values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (0, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table ${table2} with sync;"""
    sql """analyze table ${table1} with sync;"""

    def create_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1') 
        AS  
        ${mv_sql}
        """
    }

    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def param1 = "2"
    def param2 = "l_orderkey"
    def param3 = "abs(l_orderkey)"
    def param4 = "2+l_orderkey"
    def param5 = "2+abs(l_orderkey)"
    def param6 = "l_orderkey + abs(l_orderkey)"
    def param_lists1 = [param1, param2, param3, param4, param5, param6]

    def param7 = "2"
    def param8 = "o_orderkey"
    def param9 = "abs(o_orderkey)"
    def param10 = "2+o_orderkey"
    def param11 = "2+abs(o_orderkey)"
    def param12 = "o_orderkey + abs(o_orderkey)"
    def param_lists2 = [param7, param8, param9, param10, param11, param12]

    def sql8 = "select plac_1 from ${table1} left join (select plac_1 as col1 from ${table1}) as t1 on plac_1 = t1.col1 "
    def sql_lists = [sql8]

    def check_not_chose = { def str, def mv_name ->
        def sql_explain = sql """explain ${str};"""
        def mv_index_1 = sql_explain.toString().indexOf("MaterializedViewRewriteSuccessAndChose:")
        def mv_index_2 = sql_explain.toString().indexOf("MaterializedViewRewriteFail:")
        assert(mv_index_1 != -1)
        assert(mv_index_2 != -1)
        if (sql_explain.toString().substring(mv_index_1, mv_index_2).indexOf(mv_name) != -1) {
            return true
        }
        return false
    }

    for (int i = 0; i < sql_lists.size(); i++) {
        for (int j = 0; j < param_lists1.size(); j++) {
            for (int k = 0; k < param_lists2.size(); k++) {
                logger.info("i: " + i + ", j: " + j + ", k: " + k)
                def str = sql_lists[i].replaceAll("plac_1", param_lists1[j]).replaceAll("plac_2", param_lists2[k]).replaceAll(" on 2 = ", " on l_orderkey = ")

                create_mv(mtmv_name, str)
                waitingMTMVTaskFinishedByMvName(mtmv_name)

                check_not_chose(str, mtmv_name)
                compare_res(str + " order by 1")
            }
        }
    }
    
}
