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

suite("test_paimon_null_partition_mtmv_refresh", "p0,external,mtmv,external_docker,external_docker_doris") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        return
    }

    String dbName = context.config.getDbNameByFile(context.file)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP MATERIALIZED VIEW IF EXISTS paimon_null_partition_mtmv"
    sql "DROP CATALOG IF EXISTS paimon_null_partition_mtmv_catalog"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.paimon_null_partition_mtmv_db;
        DROP TABLE IF EXISTS paimon.paimon_null_partition_mtmv_db.events;
        CREATE TABLE paimon.paimon_null_partition_mtmv_db.events (id INT, region STRING)
        USING paimon PARTITIONED BY (region);
        INSERT INTO paimon.paimon_null_partition_mtmv_db.events VALUES (1, NULL), (2, 'null');
    """

    // Read the latest snapshot after the Spark commit without an explicit metadata refresh.
    sql """
        CREATE CATALOG paimon_null_partition_mtmv_catalog PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh/',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.region' = 'us-east-1',
            'meta.cache.paimon.table.ttl-second' = '0'
        )
    """
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    sql "ANALYZE TABLE paimon_null_partition_mtmv_catalog.paimon_null_partition_mtmv_db.events WITH SYNC"
    // As in the other Paimon rewrite suite, make the MV cheaper than scanning the base fixture.
    sql """
        ALTER TABLE paimon_null_partition_mtmv_catalog.paimon_null_partition_mtmv_db.events
        MODIFY COLUMN region SET STATS ('row_count' = '20')
    """
    sql """
        CREATE MATERIALIZED VIEW paimon_null_partition_mtmv
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY (region)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS SELECT region, COUNT(*) AS row_count
        FROM paimon_null_partition_mtmv_catalog.paimon_null_partition_mtmv_db.events
        GROUP BY region
    """

    String nullRegionQuery = """
        SELECT region, COUNT(*) AS row_count
        FROM paimon_null_partition_mtmv_catalog.paimon_null_partition_mtmv_db.events
        WHERE region IS NULL
        GROUP BY region
        ORDER BY region
    """
    String baseNullRegionQuery = nullRegionQuery.replace("SELECT region",
            "SELECT /*+ SET_VAR(enable_materialized_view_rewrite=false) */ region")
    String mvNullRegionQuery = """
        SELECT region, row_count FROM paimon_null_partition_mtmv WHERE region IS NULL ORDER BY region
    """

    sql "REFRESH MATERIALIZED VIEW paimon_null_partition_mtmv AUTO"
    waitingMTMVTaskFinishedByMvName("paimon_null_partition_mtmv")
    quickTest("initial_null_partition", mvNullRegionQuery, baseNullRegionQuery)
    mv_rewrite_success(nullRegionQuery, "paimon_null_partition_mtmv")

    spark_paimon "INSERT INTO paimon.paimon_null_partition_mtmv_db.events VALUES (3, NULL)"

    // A query restricted to the now-stale NULL partition must read current base data.
    explain {
        sql("memo plan ${nullRegionQuery}")
        notContains(".paimon_null_partition_mtmv chose")
        notContains(".paimon_null_partition_mtmv not chose")
    }
    quickTest("null_partition_while_stale", nullRegionQuery, baseNullRegionQuery)

    sql "REFRESH MATERIALIZED VIEW paimon_null_partition_mtmv AUTO"
    waitingMTMVTaskFinishedByMvName("paimon_null_partition_mtmv")

    // The automatic refresh must touch only the SQL NULL partition and leave literal 'null' alone.
    quickTest("refreshed_null_partition", """
        SELECT json_extract_string(NeedRefreshPartitions, '\$[0]'),
               json_extract_string(CompletedPartitions, '\$[0]')
        FROM (
            SELECT RefreshMode, NeedRefreshPartitions, CompletedPartitions
            FROM tasks('type' = 'mv')
            WHERE MvDatabaseName = '${dbName}' AND MvName = 'paimon_null_partition_mtmv'
            ORDER BY CreateTime DESC, TaskId DESC
            LIMIT 1
        ) latest
        WHERE RefreshMode = 'PARTIAL'
          AND JSON_LENGTH(NeedRefreshPartitions) = 1
          AND JSON_LENGTH(CompletedPartitions) = 1
    """, """
        SELECT MAX(PARTITION_NAME), MAX(PARTITION_NAME) FROM information_schema.partitions
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'paimon_null_partition_mtmv'
          AND PARTITION_NAME = 'pn_NULL'
    """)

    quickTest("incremental_null_partition", mvNullRegionQuery, baseNullRegionQuery)
    mv_rewrite_success(nullRegionQuery, "paimon_null_partition_mtmv")
    quickTest("null_partition_after_rewrite", nullRegionQuery, baseNullRegionQuery)
}
