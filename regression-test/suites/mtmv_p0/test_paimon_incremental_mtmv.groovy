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

suite("test_paimon_incremental_mtmv", "p0,external,mtmv,external_docker,external_docker_doris") {
    logger.info("start test_paimon_incremental_mtmv")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }
    String suiteName = "test_paimon_mtmv"
    String catalogName = "${suiteName}_catalog"
    String mvName = "${suiteName}_incremental_mv"
    String dbName = context.config.getDbNameByFile(context.file)
    String otherDbName = "${suiteName}_otherdb"
    String tableName = "${suiteName}_table"
    String paimonDbName = "test_paimon_spark"
    String paimonAggTableName = "paimon_incremental_mv_test_agg_table"

    sql """drop database if exists ${otherDbName}"""
    sql """create database ${otherDbName}"""
    sql """
        CREATE TABLE ${otherDbName}.${tableName} (
          c1 INT,
          c2 INT
        ) ENGINE=OLAP
        PROPERTIES ('replication_num' = '1')
       """

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """DROP CATALOG IF EXISTS ${catalogName}"""
    sql """CREATE CATALOG ${catalogName} PROPERTIES (
            'type'='paimon',
            'warehouse' = 's3://warehouse/wh/',
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
            "s3.region" = "us-east-1",
            "fs.oss.connection.timeout" = "1000",
            "fs.oss.connection.establish.timeout" = "1000"
        );"""

    order_qt_base_agg_table """select * from ${catalogName}.${paimonDbName}.${paimonAggTableName} order by date asc"""

    String mvSql = "SELECT date, k1, sum(a1) as a1\n" +
            "FROM ${catalogName}.${paimonDbName}.${paimonAggTableName}\n" +
            "GROUP BY 1, 2"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            ${mvSql}
        """
    // mv is a agg table, column date,k1 is primary key, a1 is function sum
    order_qt_mv_struct "DESC ${mvName}"

    // first refresh
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_first_init "SELECT * FROM ${mvName}"

    // refresh one partition
    sql """REFRESH MATERIALIZED VIEW ${mvName} PARTITIONS(p_20250101)"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_one_partition "SELECT * FROM ${mvName}"

    // refresh complete
    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_complete "SELECT * FROM ${mvName}"

    // test rewrite
    sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
    def explainOnePartition = sql """ explain  ${mvSql} """
    logger.info("explainOnePartition: " + explainOnePartition.toString())
    assertTrue(explainOnePartition.toString().contains("${mvName} chose"))
    order_qt_sql_rewrite "${mvSql}"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName}"""

    // mv sql must be agg mode，such as Agg -> project -> FileScan
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select date, k1, a1
            FROM ${catalogName}.${paimonDbName}.${paimonAggTableName}
            """
        exception "mv query must be agg mode on external table"
    }

    // mv sql base table must paimon table
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select c1, sum(c2) as c2
            FROM ${otherDbName}.${tableName}
            GROUP BY 1
            """
        exception "mv query must be agg mode on external table"
    }

    // mv sql complex expression column must has alias, such as sum(a) as x
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select date, k1 + 1, sum(a1) as a1
            FROM ${catalogName}.${paimonDbName}.${paimonAggTableName}
            GROUP BY 1, 2
            """
        exception "complex expression must has alias"
    }
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select date, k1, sum(a1)
            FROM ${catalogName}.${paimonDbName}.${paimonAggTableName}
            GROUP BY 1, 2
            """
        exception "complex expression must has alias"
    }

    // mv sql agg function args must paimon agg columns
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select date, sum(k1) as k1
            FROM ${catalogName}.${paimonDbName}.${paimonAggTableName}
            GROUP BY 1
            """
        exception "Unable to find a reasonable aggregation function"
    }

    // mv sql agg function and args must same as paimon agg function and args
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select date, max(a1) as a1
            FROM ${catalogName}.${paimonDbName}.${paimonAggTableName}
            GROUP BY 1
            """
        exception "different from paimon agg function"
    }

    // mv sql group by columns must be paimon table primary keys
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            select a1
            FROM ${catalogName}.${paimonDbName}.${paimonAggTableName}
            GROUP BY 1
            """
        exception "exist non-primary key group columns"
    }
}

