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

suite("mv_contain_external_table", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test. then doesn't test mv rewrite")
        return;
    }
    // prepare table and data in hive
    def hive_database = "test_mv_contain_external_table_rewrite_db"
    def hive_table = "orders"

    def autogather_off_str = """ set hive.stats.column.autogather = false; """
    def autogather_on_str = """ set hive.stats.column.autogather = true; """
    def drop_table_str = """ drop table if exists ${hive_database}.${hive_table} """
    def drop_database_str = """ drop database if exists ${hive_database}"""
    def create_database_str = """ create database ${hive_database}"""
    def create_table_str = """CREATE TABLE ${hive_database}.${hive_table} (
                                    o_orderkey INT,
                                    o_custkey INT,
                                    o_orderstatus STRING,
                                    o_totalprice DECIMAL(15, 2),
                                    o_orderpriority STRING,
                                    o_clerk STRING,
                                    o_shippriority INT,
                                    o_comment STRING
                                )
                                PARTITIONED BY (o_orderdate STRING)
                                STORED AS ORC;"""
    def add_partition_1_str = """
                                alter table ${hive_database}.${hive_table} add if not exists
                                partition(o_orderdate='2023-10-17');
                            """
    def add_partition_2_str = """
                                alter table ${hive_database}.${hive_table} add if not exists
                                partition(o_orderdate='2023-10-18');
                            """
    def add_partition_3_str = """
                                alter table ${hive_database}.${hive_table} add if not exists
                                partition(o_orderdate='2023-10-19');
                            """

    def insert_str1 = """ insert into ${hive_database}.${hive_table} 
    PARTITION(o_orderdate='2023-10-17') values(1, 1, 'ok', 99.5, 'a', 'b', 1, 'yy')"""
    def insert_str2 = """ insert into ${hive_database}.${hive_table} 
    PARTITION(o_orderdate='2023-10-18') values(2, 2, 'ok', 109.2, 'c','d',2, 'mm')"""
    def insert_str3 = """ insert into ${hive_database}.${hive_table} 
    PARTITION(o_orderdate='2023-10-19') values(3, 3, 'ok', 99.5, 'a', 'b', 1, 'yy')"""

    hive_docker """ ${autogather_off_str} """
    hive_docker """ ${drop_table_str} """
    hive_docker """ ${drop_database_str} """
    hive_docker """ ${create_database_str}"""
    hive_docker """ ${create_table_str} """
    hive_docker """ ${add_partition_1_str} """
    hive_docker """ ${add_partition_2_str} """
    hive_docker """ ${add_partition_3_str} """
    hive_docker """ ${insert_str1} """
    hive_docker """ ${insert_str2} """
    hive_docker """ ${insert_str3} """


    // prepare catalog
    String hms_port = context.config.otherConfigs.get("hive2HmsPort")
    String catalog_name = "hive_test_mv_rewrite"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type"="hms",
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
    );"""


    // prepare olap table and data
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"

    sql """
    drop table if exists lineitem
    """

    sql """
    CREATE TABLE IF NOT EXISTS lineitem (
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
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into lineitem values 
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx');
    """

    def query_sql = """
            select l_orderkey, l_partkey, o_custkey, l_shipdate
            from lineitem
            left join ${catalog_name}.${hive_database}.${hive_table} on l_orderkey = o_orderkey;
    """

    order_qt_query_sql """${query_sql}"""

    // create mv
    def mv_name = 'mv_join'
    sql """drop materialized view if exists ${mv_name}"""
    sql """
        CREATE MATERIALIZED VIEW ${mv_name}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`l_shipdate`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1', 'grace_period' = '0')
            AS ${query_sql}
        """

    sql """REFRESH MATERIALIZED VIEW ${mv_name} complete"""
    waitingMTMVTaskFinished(getJobName(db, mv_name))

    order_qt_query_mv_directly """select * from ${mv_name};"""

    // test query rewrite by mv, should fail ,because materialized_view_rewrite_enable_contain_external_table
    // switch is false default
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
    hive_docker """ ${insert_str3} """
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_rewritten_with_old_data """ ${query_sql}"""

    // refresh manually
    sql """REFRESH catalog ${catalog_name}"""
    sql """REFRESH MATERIALIZED VIEW ${mv_name} complete"""
    waitingMTMVTaskFinished(getJobName(db, mv_name))
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_rewritten_with_new_data """ ${query_sql}"""


    // hive add partition
    def add_partition_10_20 = """alter table ${hive_database}.${hive_table} add if not exists partition(o_orderdate='2023-10-20');"""
    hive_docker """ ${add_partition_10_20} """
    def insert_str4 = """ insert into ${hive_database}.${hive_table} 
    PARTITION(o_orderdate='2023-10-20') values(3, 3, 'ok', 100.5, 'f', 'h', 3, 'ss')"""
    hive_docker """ ${insert_str4} """
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_rewritten_with_old_data_after_add_partition """ ${query_sql}"""

    sql """REFRESH catalog ${catalog_name}"""
    sql """REFRESH MATERIALIZED VIEW ${mv_name} complete"""
    waitingMTMVTaskFinished(getJobName(db, mv_name))
    explain {
        sql(""" ${query_sql}""")
        contains("${mv_name}(${mv_name})")
    }
    order_qt_query_rewritten_with_new_data """ ${query_sql}"""

    hive_docker """ ${autogather_on_str} """
    sql """drop materialized view if exists ${mv_name};"""
    sql """drop catalog if exists ${catalog_name}"""
}
