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

suite("bitmap_allowlist_scan_iceberg", "p2,external,iceberg,external_docker,external_docker_iceberg,nonConcurrent") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.toString()?.equalsIgnoreCase("true")) {
        return
    }

    String catalog = "bitmap_allowlist_benchmark"
    String database = "bitmap_allowlist_benchmark"
    String factTable = "${catalog}.${database}.bitmap_allowlist_fact"
    String allowlistTable = "internal.${context.dbName}.bitmap_allowlist_ids"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "DROP TABLE IF EXISTS bitmap_allowlist_ids"
    sql """
        CREATE TABLE bitmap_allowlist_ids (
            resource_id BIGINT NOT NULL,
            first_size INT NOT NULL
        ) DUPLICATE KEY(resource_id)
        DISTRIBUTED BY HASH(resource_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO bitmap_allowlist_ids
        SELECT 4294967296 + number,
               CASE WHEN number % 80 = 0 THEN 250000
                    WHEN number % 20 = 0 THEN 1000000
                    WHEN number % 4 = 0 THEN 5000000
                    ELSE 10000000 END
        FROM numbers("number" = "20000000")
        WHERE number % 2 = 0
    """

    sql "DROP CATALOG IF EXISTS ${catalog}"
    sql """
        CREATE CATALOG ${catalog} PROPERTIES (
            "type" = "iceberg", "iceberg.catalog.type" = "rest",
            "uri" = "http://${host}:${restPort}",
            "s3.endpoint" = "http://${host}:${minioPort}",
            "s3.access_key" = "admin", "s3.secret_key" = "password",
            "s3.region" = "us-east-1", "use_path_style" = "true"
        )
    """
    sql "SWITCH ${catalog}"
    try {
        sql "CREATE DATABASE IF NOT EXISTS ${database}"
        sql "USE ${database}"
        sql "DROP TABLE IF EXISTS bitmap_allowlist_fact"
        sql """
            CREATE TABLE bitmap_allowlist_fact (
                org_id BIGINT,
                resource_id BIGINT,
                payload STRING
            ) PARTITION BY LIST(org_id) ()
        """
    } finally {
        sql "SWITCH internal"
    }
    sql "SET enable_file_scanner_v2 = true"

    // Match the native benchmark's IDs, tenant overlap, and payload selectivity.
    for (long orgId : [101L, 202L]) {
        long rowCount = orgId == 101L ? 20000000L : 10000000L
        sql """
            INSERT INTO ${factTable}
            SELECT ${orgId}, 4294967296 + number,
                   CONCAT(IF(number % 97 = 0, 'needle', 'other'), '_',
                          CAST(number AS STRING), REPEAT('x', 64))
            FROM numbers("number" = "${rowCount}")
        """
    }

    explain {
        sql "SELECT COUNT(*) FROM ${factTable} WHERE org_id = 101"
        contains "partition=1/2"
    }

    for (long size : [250000L, 1000000L, 5000000L, 10000000L]) {
        test {
            sql """
                SELECT COUNT(*) AS n FROM ${allowlistTable}
                WHERE first_size <= ${size}
                HAVING n <> ${size}
            """
            rowNum 0
        }
        String bitmap = sql("""
            SELECT bitmap_to_base64(bitmap_agg(resource_id))
            FROM ${allowlistTable} WHERE first_size <= ${size}
        """)[0][0]
        String bitmapQuery = """
            SELECT COUNT(*) AS n, SUM(resource_id) AS s FROM ${factTable}
            WHERE org_id = 101
              AND bitmap_contains(bitmap_from_base64('${bitmap}'), resource_id)
              AND payload LIKE '%needle%'
        """
        String joinQuery = """
            SELECT COUNT(*) AS n, SUM(resource_id) AS s FROM ${factTable}
            WHERE org_id = 101
              AND resource_id IN (SELECT resource_id FROM ${allowlistTable}
                                  WHERE first_size <= ${size})
              AND payload LIKE '%needle%'
        """

        test {
            sql """
                SELECT b.n FROM (${bitmapQuery}) b, (${joinQuery}) j
                WHERE b.n <> j.n OR b.s <> j.s
            """
            rowNum 0
        }
        logger.info("Iceberg bitmap allowlist size ${size}: SQL 1/4 = bitmap, SQL 2/3 = semi-join; "
                + "bitmap Base64 bytes = ${bitmap.length()}")
        benchmark {
            warmUp true
            executeTimes 5
            skipFailure false
            sqls([bitmapQuery, joinQuery, joinQuery, bitmapQuery])
        }
    }
}
