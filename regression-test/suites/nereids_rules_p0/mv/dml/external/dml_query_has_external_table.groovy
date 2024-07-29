package mv.dml.external
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

suite("dml_query_has_external_table") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    logger.info("enabled: " + enabled)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    logger.info("externalEnvIp: " + externalEnvIp)
    String mysql_port = context.config.otherConfigs.get("mysql_57_port");
    logger.info("mysql_port: " + mysql_port)
    String s3_endpoint = getS3Endpoint()
    logger.info("s3_endpoint: " + s3_endpoint)
    String bucket = getS3BucketName()
    logger.info("bucket: " + bucket)
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
    logger.info("driver_url: " + driver_url)
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "mysql_mtmv_catalog";
        String mvName = "test_mysql_mtmv"
        String dbName = "regression_test_mtmv_p0"
        String mysqlDb = "doris_test"
        String mysqlTable = "ex_tb2"
        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/${mysqlDb}?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        // prepare olap table and data
        String db = context.config.getDbNameByFile(context.file)
        sql "use ${db}"
        sql "SET enable_nereids_planner=true"
        sql "set runtime_filter_mode=OFF";
        sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"


     sql """
    drop table if exists insert_target_olap_table
    """

    sql """
    CREATE TABLE IF NOT EXISTS insert_target_olap_table (
      id     INTEGER NOT NULL,
      count_value     varchar(100) NOT NULL
    )
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 3
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


        def insert_into_async_mv_name_external = 'orders_agg'
        def insert_into_async_query_external = """
        select
        id,
        count_value
        from
        ${catalog_name}.${mysqlDb}.${mysqlTable}
        group by
        id,
        count_value;
        """

        create_async_mv(insert_into_async_mv_name_external, """
        select
        id,
        count_value
        from
        ${catalog_name}.${mysqlDb}.${mysqlTable}
        group by
        id,
        count_value;
        """)

        // disable query rewrite by mv
        sql "set enable_materialized_view_rewrite=false";
        // enable dml rewrite by mv
        sql "set enable_dml_materialized_view_rewrite=true";
        sql "set enable_dml_materialized_view_rewrite_when_base_table_data_unawareness=false";

        explain {
            sql """
            insert into insert_target_olap_table 
            ${insert_into_async_query_external}"""
            check {result ->
                !result.contains(insert_into_async_mv_name_external)
            }
        }

        sql "set enable_dml_materialized_view_rewrite_when_base_table_data_unawareness=true";
        explain {
            sql """
            insert into insert_target_olap_table 
            ${insert_into_async_query_external}
            """
            check {result ->
                def splitResult = result.split("MaterializedViewRewriteFail")
                splitResult.length == 2 ? splitResult[0].contains(insert_into_async_mv_name_external) : false
            }
        }

        sql """insert into insert_target_olap_table ${insert_into_async_query_external}"""
        order_qt_query_insert_into_async_mv_after "${result_test_sql}"

        sql """DROP MATERIALIZED VIEW IF EXISTS ${insert_into_async_mv_name_external}"""
        sql """drop catalog if exists ${catalog_name}"""
    }
}
