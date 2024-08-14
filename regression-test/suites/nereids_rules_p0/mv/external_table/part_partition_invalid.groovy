package mv.external_table
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

suite("part_partition_invalid", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test. then doesn't test mv rewrite")
        return;
    }
    // prepare catalog
    def suite_name = "part_partition_invalid";
    def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def hms_port = context.config.otherConfigs.get("hive2HmsPort")
    def hive_catalog_name = "${suite_name}_catalog"
    def hive_database = "${suite_name}_db"
    def hive_table = "${suite_name}_orders"

    sql """drop catalog if exists ${hive_catalog_name}"""
    sql """
    create catalog if not exists ${hive_catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""

    sql """switch ${hive_catalog_name};"""
    sql """drop table if exists ${hive_catalog_name}.${hive_database}.${hive_table}"""
    sql """ drop database if exists ${hive_database}"""
    sql """ create database ${hive_database}"""
    sql """use ${hive_database}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${hive_table}  (
              o_orderkey       integer,
              o_custkey        integer,
              o_orderstatus    char(1),
              o_totalprice     decimalv3(15,2),
              o_orderpriority  char(15),  
              o_clerk          char(15), 
              o_shippriority   integer,
              o_comment        varchar(79),
              o_orderdate      date
            ) ENGINE=hive
            PARTITION BY list(o_orderdate)()
            PROPERTIES (
              "replication_num" = "1",
              "file_format"="orc",
              "compression"="zlib"
            );
            """

    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(1, 1, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-17');"""
    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(2, 2, 'ok', 109.2, 'c','d',2, 'mm', '2023-10-18');"""
    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(3, 3, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');"""

    // prepare table and data in olap
    def internal_catalog = "internal"
    def olap_db = context.config.getDbNameByFile(context.file)
    def olap_table = "${suite_name}_lineitem"

    sql """switch ${internal_catalog};"""
    sql "use ${olap_db};"
    sql "SET enable_nereids_planner=true;"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject';"

    sql """
    drop table if exists ${olap_table}
    """

    sql """
    CREATE TABLE IF NOT EXISTS ${olap_table} (
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
    (FROM ('2023-10-01') TO ('2023-10-30') INTERVAL 1 DAY)
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into ${olap_table} values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """

    def query_sql = """
            select l_orderkey, l_partkey, o_custkey, l_shipdate, o_orderdate 
            from ${hive_catalog_name}.${hive_database}.${hive_table} 
            left join ${internal_catalog}.${olap_db}.${olap_table} on l_orderkey = o_orderkey 
    """
    order_qt_query_sql """${query_sql}"""

    // create partition mtmv, related partition is hive catalog
    def mv_name = 'mv_join'
    sql """drop materialized view if exists ${mv_name}"""
    sql """
        CREATE MATERIALIZED VIEW ${mv_name}
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL
            partition by(`o_orderdate`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1', 'grace_period' = '0')
            AS ${query_sql}
        """

    sql """REFRESH MATERIALIZED VIEW ${mv_name} complete"""
    waitingMTMVTaskFinished(getJobName(olap_db, mv_name))
    order_qt_query_mv_directly """select * from ${mv_name};"""

    // test query rewrite by mv, should fail ,because materialized_view_rewrite_enable_contain_external_table
    // is false default
    explain {
        sql(""" ${query_sql}""")
        notContains("${mv_name}(${mv_name})")
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table=true"
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }

    // data change in external table doesn't influence query rewrite,
    // if want to use new data in external table should be refresh manually
    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(3, 3, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-19');"""
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_modify_data_without_refresh_catalog """ ${query_sql}"""

    explain {
        sql("""
            ${query_sql} where o_orderdate = '2023-10-19';
        """)
        // query invalid partition data, should hit mv, because not check now.
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_modify_and_without_refresh_catalog_19 """ ${query_sql} where o_orderdate = '2023-10-19';"""

    explain {
        sql("""
            ${query_sql} where o_orderdate = '2023-10-18';
        """)
        // query valid partition data, should hit mv
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_modify_and_without_refresh_catalog_18 """ ${query_sql} where o_orderdate = '2023-10-18';"""

    // refresh catalog cache
    sql """ REFRESH CATALOG ${hive_catalog_name} PROPERTIES("invalid_cache" = "true"); """
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_modify_data_and_refresh_catalog """ ${query_sql}"""

    explain {
        sql("""
            ${query_sql} where o_orderdate = '2023-10-19';
        """)
        // query invalid partition data, should hit mv, because not check now.
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_modify_and_refresh_catalog_19 """ ${query_sql} where o_orderdate = '2023-10-19';"""

    explain {
        sql("""
            ${query_sql} where o_orderdate = '2023-10-18';
        """)
        // query valid partition data, should hit mv
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_modify_and_refresh_catalog_18 """ ${query_sql} where o_orderdate = '2023-10-18';"""

    // refresh manually
    sql """ REFRESH CATALOG ${hive_catalog_name} PROPERTIES("invalid_cache" = "true");  """
    sql """REFRESH MATERIALIZED VIEW ${mv_name} auto"""
    waitingMTMVTaskFinished(getJobName(olap_db, mv_name))
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_modify_data_and_refresh_catalog_and_mv """ ${query_sql}"""

    // test after hive add partition
    sql """insert into ${hive_catalog_name}.${hive_database}.${hive_table} values(6, 7, 'ok', 29.5, 'x', 'y', 6, 'ss', '2023-10-20');"""
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_add_data_without_refresh_catalog """ ${query_sql}"""

    explain {
        sql("""
            ${query_sql}
        """)
        // query invalid partition data, should hit mv, because not check now.
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_add_and_without_refresh_catalog_19 """ ${query_sql} where o_orderdate = '2023-10-19';"""

    explain {
        sql("""
            ${query_sql} where o_orderdate = '2023-10-20';
        """)
        // query valid partition data, should hit mv
        notContains("${mv_name}(${mv_name})")
    }
    order_qt_after_add_and_without_refresh_catalog_20 """ ${query_sql} where o_orderdate = '2023-10-20';"""

    // refresh catalog cache
    sql """ REFRESH CATALOG ${hive_catalog_name} PROPERTIES("invalid_cache" = "true"); """
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_add_data_with_refresh_catalog """ ${query_sql}"""

    explain {
        sql("""
            ${query_sql} where o_orderdate = '2023-10-19';
        """)
        // query invalid partition data, should hit mv, because not check now.
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_add_and_refresh_catalog_19 """ ${query_sql} where o_orderdate = '2023-10-19';"""

    explain {
        sql("""
            ${query_sql} where o_orderdate = '2023-10-20';
        """)
        // query valid partition data, should hit mv
        notContains("${mv_name}(${mv_name})")
    }
    order_qt_after_add_and_refresh_catalog_20 """ ${query_sql} where o_orderdate = '2023-10-20';"""

    // refresh manually
    sql """ REFRESH CATALOG ${hive_catalog_name} PROPERTIES("invalid_cache" = "true");  """
    sql """REFRESH MATERIALIZED VIEW ${mv_name} auto"""
    waitingMTMVTaskFinished(getJobName(olap_db, mv_name))
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_after_add_data_and_refresh_catalog_and_mv """ ${query_sql}"""

    sql """drop table if exists ${hive_catalog_name}.${hive_database}.${hive_table}"""
    sql """drop table if exists ${internal_catalog}.${olap_db}.${olap_table}"""
    sql """drop database if exists ${hive_catalog_name}.${hive_database}"""
    sql """drop materialized view if exists ${internal_catalog}.${olap_db}.${mv_name};"""
    sql """drop catalog if exists ${hive_catalog_name}"""
}
