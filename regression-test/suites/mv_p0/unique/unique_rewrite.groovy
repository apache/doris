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

suite("mv_on_unique_table") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists lineitem_2_uniq;
    """

    sql """
    CREATE TABLE `lineitem_2_uniq` (
      `l_orderkey` BIGINT NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_suppkey` INT NULL,
      `l_shipdate` DATE not NULL,
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
      `l_comment` VARCHAR(44) NULL
    ) ENGINE=OLAP
    unique KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate )
    COMMENT 'OLAP'
    auto partition by range (date_trunc(`l_shipdate`, 'day')) ()
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    insert into lineitem_2_uniq values 
    (null, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (null, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, null, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (1, null, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 3, null, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (3, 3, null, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
    (1, 2, 3, null, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 2, 3, null, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
    (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
    (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy');
    """

    // delete some data to check the doris_delete_sign is useful or not
    sql """delete from lineitem_2_uniq where l_orderkey = 1;"""

    sql""" analyze table lineitem_2_uniq with sync;"""

    // test partition prune in duplicate table
    def mv1 = """
        select l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate,
        substring(concat(l_returnflag, l_linestatus), 1)
        from lineitem_2_uniq;
    """

    def query1 = """
        select l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate,
        substring(concat(l_returnflag, l_linestatus), 1)
        from lineitem_2_uniq;
    """

    order_qt_query1_before "${query1}"
    createMV("""
    CREATE MATERIALIZED VIEW mv1
    AS
    ${mv1}
    """)
    explain {
        sql("""${query1}""")
        check {result ->
            result.contains("(mv1)") && result.contains("__DORIS_DELETE_SIGN__")
        }
    }
    order_qt_query1_after "${query1}"
    sql """drop materialized view mv1 on lineitem_2_uniq;"""

    // test partition prune in unique table
    def mv2 = """
        select l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate,
        substring(concat(l_returnflag, l_linestatus), 1)
        from lineitem_2_uniq;
    """

    def query2 = """
        select l_orderkey, l_suppkey,
        substring(concat(l_returnflag, l_linestatus), 1)
        from lineitem_2_uniq;
    """

    order_qt_query2_before "${query2}"
    createMV("""
    CREATE MATERIALIZED VIEW mv2
    AS
    ${mv2}
    """)
    explain {
        sql("""${query2}""")
        check {result ->
            result.contains("(mv2)") && result.contains("__DORIS_DELETE_SIGN__")
        }
    }
    order_qt_query2_after "${query2}"
    sql """drop materialized view mv2 on lineitem_2_uniq;"""
}
