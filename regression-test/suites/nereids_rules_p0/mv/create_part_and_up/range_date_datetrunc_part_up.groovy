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

suite("mtmv_range_date_datetrunc_date_part_up") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    String mv_prefix = "range_datetrunc_date_up"
    String tb_name = mv_prefix + "_tb"
    String mv_name = mv_prefix + "_mv"

    sql """
    drop table if exists ${tb_name}
    """

    sql """CREATE TABLE `${tb_name}` (
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
      `l_shipdate` DATEtime not NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    partition by range (`l_shipdate`) (
        partition p1 values [("2023-10-29 00:00:00"), ("2023-10-29 01:00:00")),
        partition p2 values [("2023-10-29 01:00:00"), ("2023-10-29 02:00:00")),
        partition p3 values [("2023-10-29 02:00:00"), ("2023-10-29 03:00:00")),
        partition p4 values [("2023-10-29 03:00:00"), ("2023-10-29 04:00:00")),
        partition p5 values [("2023-10-29 04:00:00"), ("2023-10-29 05:00:00"))
    )
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into ${tb_name} values 
    (null, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 00:00:00'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29 01:00:00'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-29 02:00:00'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-29 03:00:00'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-29 04:00:00');
    """

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

    def create_mv = { cur_mv_name, mv_sql, col_name, date_trunc_range ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${cur_mv_name};"""
        sql """DROP TABLE IF EXISTS ${cur_mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${cur_mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        partition by(date_trunc(`${col_name}`, '${date_trunc_range}')) 
        DISTRIBUTED BY RANDOM BUCKETS 2 
        PROPERTIES ('replication_num' = '1')  
        AS  
        ${mv_sql}
        """
    }

    def select_list1_1 = "l_shipdate"
    def select_list1_2 = "date_trunc(`l_shipdate`, 'day') as col1"
    def select_list1_3 = "DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list2_1 = "date_trunc(`l_shipdate`, 'day') as col1, l_shipdate"
    def select_list2_2 = "date_trunc(`l_shipdate`, 'day') as col1, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list2_3 = "l_shipdate, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list3_1 = "date_trunc(`l_shipdate`, 'day') as col1, l_shipdate, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d')"
    def select_list3_2 = "date_trunc(`l_shipdate`, 'day') as col1, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d'), l_shipdate"
    def select_list3_3 = "l_shipdate, DATE_FORMAT(`l_shipdate`, '%Y-%m-%d'), date_trunc(`l_shipdate`, 'day') as col1"

    def select_lists = [select_list1_1, select_list1_2, select_list1_3, select_list2_1, select_list2_2,
                        select_list2_3, select_list3_1, select_list3_2, select_list3_3]
    for (int i = 0; i < select_lists.size(); i++) {
        for (int j = 0; j < select_lists.size(); j++) {
            if (i == j || j > 5) {
                def group_by_str = select_lists[j].replaceAll("as col1", "")
                def str = "select " + select_lists[i] + " from ${tb_name} group by " + group_by_str
                sql str

                if (select_lists[i].replaceAll("`l_shipdate`", "").indexOf("l_shipdate") != -1) {
                    create_mv(mv_name, str, "l_shipdate", "day")
                    waitingMTMVTaskFinishedByMvName(mv_name)
                    mv_rewrite_success(str, mv_name)
                    compare_res(str + " order by 1,2,3")
                }

                if (select_lists[i].indexOf("col1") != -1) {
                    create_mv(mv_name, str, "col1", "day")
                    waitingMTMVTaskFinishedByMvName(mv_name)
                    mv_rewrite_success(str, mv_name)
                    compare_res(str + " order by 1,2,3")
                }

            }
        }
    }

}
