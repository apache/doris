package mv.dml.insert
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

suite("dml_insert_and_overwrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists partsupp
    """

    sql"""
    CREATE TABLE IF NOT EXISTS partsupp (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    insert into partsupp values
    (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    sql """analyze table partsupp with sync;"""


    sql """
    drop table if exists insert_target_olap_table
    """
    sql """
    CREATE TABLE IF NOT EXISTS insert_target_olap_table (
      ps_partkey     INTEGER NOT NULL,
      ps_suppkey     INTEGER NOT NULL,
      ps_availqty    INTEGER NOT NULL,
      ps_supplycost  DECIMALV3(15,2)  NOT NULL,
      ps_comment     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(ps_partkey, ps_suppkey)
    DISTRIBUTED BY HASH(ps_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    def create_async_mv = { mv_name, mv_sql ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """
        waitingMTMVTaskFinished(getJobName(db, mv_name))
    }

    def result_test_sql = """select * from insert_target_olap_table;"""


    // 1. test insert into olap table when async mv
    def insert_into_async_mv_name = 'partsupp_agg'
    def insert_into_async_query = """
        select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;
    """
    create_async_mv(insert_into_async_mv_name,
    """select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;""")

    // disable query rewrite by mv
    sql "set enable_materialized_view_rewrite=false";
    // enable dml rewrite by mv
    sql "set enable_dml_materialized_view_rewrite=true";

    explain {
        sql """insert into insert_target_olap_table
            ${insert_into_async_query}"""
        check {result ->
            def splitResult = result.split("MaterializedViewRewriteFail")
            splitResult.length == 2 ? splitResult[0].contains(insert_into_async_mv_name) : false
        }
    }

    sql """insert into insert_target_olap_table ${insert_into_async_query}"""
    order_qt_query_insert_into_async_mv_after "${result_test_sql}"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${insert_into_async_mv_name}"""


    // 2. test insert into olap table when sync mv
    def insert_into_sync_mv_name = 'group_by_each_column_sync_mv'
    def insert_into_sync_query = """
        select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;
    """
    createMV(""" create materialized view ${insert_into_sync_mv_name}
        as select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment,
        count(*)
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;""")

    // disable query rewrite by mv
    sql "set enable_materialized_view_rewrite=false";
    // enable dml rewrite by mv
    sql "set enable_dml_materialized_view_rewrite=true";

    explain {
        sql """insert into insert_target_olap_table
            ${insert_into_sync_query}"""
        check {result ->
            def splitResult = result.split("MaterializedViewRewriteFail")
            splitResult.length == 2 ? splitResult[0].contains(insert_into_sync_mv_name) : false
        }
    }
    sql """insert into insert_target_olap_table ${insert_into_sync_query}"""

    order_qt_query_insert_into_sync_mv_after "${result_test_sql}"
    sql """drop materialized view if exists ${insert_into_sync_mv_name} on partsupp;"""


    // 3. test insert into overwrite olap table when async mv
    def insert_overwrite_async_mv_name = 'partsupp_agg'
    def insert_overwrite_async_query = """
        select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;
    """
    create_async_mv(insert_overwrite_async_mv_name,
            """select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;""")

    // disable query rewrite by mv
    sql "set enable_materialized_view_rewrite=false";
    // enable dml rewrite by mv
    sql "set enable_dml_materialized_view_rewrite=true";

    explain {
        sql """INSERT OVERWRITE table insert_target_olap_table
            ${insert_overwrite_async_query}"""
        check {result ->
            def splitResult = result.split("MaterializedViewRewriteFail")
            splitResult.length == 2 ? splitResult[0].contains(insert_overwrite_async_mv_name) : false
        }
    }

    sql """INSERT OVERWRITE table insert_target_olap_table ${insert_overwrite_async_query}"""
    order_qt_query_insert_overwrite_async_mv_after "${result_test_sql}"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${insert_overwrite_async_mv_name}"""

    // 4. test insert into overwrite olap table when sync mv
    def insert_overwrite_sync_mv_name = 'group_by_each_column_sync_mv'
    def insert_overwrite_sync_query = """
        select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;
    """
    create_async_mv(insert_overwrite_sync_mv_name,
            """select
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment
        from
        partsupp
        group by
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment;""")

    // disable query rewrite by mv
    sql "set enable_materialized_view_rewrite=false";
    // enable dml rewrite by mv
    sql "set enable_dml_materialized_view_rewrite=true";

    explain {
        sql """INSERT OVERWRITE table insert_target_olap_table
            ${insert_overwrite_sync_query}"""
        check {result ->
            def splitResult = result.split("MaterializedViewRewriteFail")
            splitResult.length == 2 ? splitResult[0].contains(insert_overwrite_sync_mv_name) : false
        }
    }

    sql """INSERT OVERWRITE table insert_target_olap_table ${insert_overwrite_sync_query}"""
    order_qt_query_insert_overwrite_sync_mv_after "${result_test_sql}"
    sql """DROP MATERIALIZED VIEW IF EXISTS ${insert_overwrite_sync_mv_name}"""

}
