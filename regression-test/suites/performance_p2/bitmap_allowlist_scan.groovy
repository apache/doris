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

suite("bitmap_allowlist_scan", "p2,nonConcurrent") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "DROP TABLE IF EXISTS bitmap_allowlist_fact"
    sql "DROP TABLE IF EXISTS bitmap_allowlist_ids"

    sql """
        CREATE TABLE bitmap_allowlist_fact (
            org_id BIGINT NOT NULL,
            resource_id BIGINT NOT NULL,
            payload VARCHAR(128) NOT NULL
        ) DUPLICATE KEY(org_id, resource_id)
        PARTITION BY LIST(org_id) (
            PARTITION target_org VALUES IN ("101"),
            PARTITION other_org VALUES IN ("202")
        )
        DISTRIBUTED BY HASH(resource_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE bitmap_allowlist_ids (
            resource_id BIGINT NOT NULL,
            first_size INT NOT NULL
        ) DUPLICATE KEY(resource_id)
        DISTRIBUTED BY HASH(resource_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """

    // Both orgs intentionally share resource IDs: the org predicate must exclude the other org.
    for (long orgId : [101L, 202L]) {
        long rowCount = orgId == 101L ? 20000000L : 10000000L
        sql """
            INSERT INTO bitmap_allowlist_fact
            SELECT ${orgId}, 4294967296 + number,
                   CONCAT(IF(number % 97 = 0, 'needle', 'other'), '_',
                          CAST(number AS STRING), REPEAT('x', 64))
            FROM numbers("number" = "${rowCount}")
        """
    }

    // Each size is a uniformly spaced subset of the same 20 million target-org IDs.
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

    explain {
        sql "SELECT COUNT(*) FROM bitmap_allowlist_fact WHERE org_id = 101"
        contains "partitions=1/2"
    }

    def factTable = "bitmap_allowlist_fact"
    for (long size : [250000L, 1000000L, 5000000L, 10000000L]) {
        test {
            sql """
                SELECT COUNT(*) AS n FROM bitmap_allowlist_ids
                WHERE first_size <= ${size}
                HAVING n <> ${size}
            """
            rowNum 0
        }
        String bitmap = sql("""
            SELECT bitmap_to_base64(bitmap_agg(resource_id))
            FROM bitmap_allowlist_ids WHERE first_size <= ${size}
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
              AND resource_id IN (SELECT resource_id FROM bitmap_allowlist_ids
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
        logger.info("Native bitmap allowlist size ${size}: SQL 1/4 = bitmap, SQL 2/3 = semi-join; "
                + "bitmap Base64 bytes = ${bitmap.length()}")
        benchmark {
            warmUp true
            executeTimes 5
            skipFailure false
            sqls [bitmapQuery, joinQuery, joinQuery, bitmapQuery]
        }
    }
}
