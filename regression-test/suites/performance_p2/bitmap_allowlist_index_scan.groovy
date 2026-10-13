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

import org.apache.doris.regression.action.ProfileAction

import java.util.regex.Pattern

suite("bitmap_allowlist_index_scan", "p2,nonConcurrent") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "SET enable_condition_cache = false"
    sql "SET enable_inverted_index_query = true"
    sql "DROP TABLE IF EXISTS bitmap_allowlist_index_fact"
    sql "DROP TABLE IF EXISTS bitmap_allowlist_plain_fact"
    sql "DROP TABLE IF EXISTS bitmap_allowlist_index_ids"
    sql "DROP TABLE IF EXISTS bitmap_allowlist_nullable_fact"

    // Keep the two layouts identical except for the resource_id inverted index.
    // resource_id is a value column so the point-query control cannot be answered by scan keys.
    sql """
        CREATE TABLE bitmap_allowlist_plain_fact (
            org_id BIGINT NOT NULL,
            resource_id BIGINT NOT NULL,
            payload VARCHAR(128) NOT NULL
        ) DUPLICATE KEY(org_id)
        PARTITION BY LIST(org_id) (
            PARTITION target_org VALUES IN ("101"),
            PARTITION other_org VALUES IN ("202")
        )
        DISTRIBUTED BY HASH(resource_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE bitmap_allowlist_index_fact (
            org_id BIGINT NOT NULL,
            resource_id BIGINT NOT NULL,
            payload VARCHAR(128) NOT NULL,
            INDEX idx_resource_id (resource_id) USING INVERTED
        ) DUPLICATE KEY(org_id)
        PARTITION BY LIST(org_id) (
            PARTITION target_org VALUES IN ("101"),
            PARTITION other_org VALUES IN ("202")
        )
        DISTRIBUTED BY HASH(resource_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE bitmap_allowlist_index_ids (
            resource_id BIGINT NOT NULL,
            first_size INT NOT NULL
        ) DUPLICATE KEY(resource_id)
        DISTRIBUTED BY HASH(resource_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE bitmap_allowlist_nullable_fact (
            org_id BIGINT NOT NULL,
            resource_id BIGINT NULL,
            payload VARCHAR(128) NOT NULL
        ) DUPLICATE KEY(org_id)
        DISTRIBUTED BY HASH(org_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO bitmap_allowlist_nullable_fact VALUES
            (101, NULL, 'needle'),
            (101, 97, 'needle'),
            (101, 4294967393, 'needle'),
            (101, 4294967393, 'needle'),
            (202, 4294967393, 'needle')
    """

    String bitmapSubquery = """
        SELECT COUNT(*) AS n FROM bitmap_allowlist_nullable_fact
        WHERE org_id = 101 AND resource_id IN (
            SELECT bitmap_agg(resource_id) FROM bitmap_allowlist_nullable_fact WHERE org_id = 202)
    """
    explain {
        sql bitmapSubquery
        contains "VNESTED LOOP JOIN"
    }
    sql "SET enable_bitmap_subquery_hash_join = true"
    explain {
        sql bitmapSubquery
        contains "VHASH JOIN"
        contains "explode_bitmap"
        contains "runtime filters"
    }
    test {
        sql "SELECT n FROM (${bitmapSubquery}) t WHERE n <> 2"
        rowNum 0
    }
    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_nullable_fact
            WHERE org_id = 101 AND resource_id IN (
                SELECT bitmap_agg(resource_id) FROM bitmap_allowlist_nullable_fact WHERE org_id = 303)
            HAVING n <> 0
        """
        rowNum 0
    }
    sql "SET enable_bitmap_subquery_hash_join = false"

    // Both orgs share IDs, including values above UINT32_MAX.
    for (long orgId : [101L, 202L]) {
        long rowCount = orgId == 101L ? 20000000L : 10000000L
        sql """
            INSERT INTO bitmap_allowlist_plain_fact
            SELECT ${orgId}, 4294967296 + number,
                   CONCAT(IF(number % 97 = 0, 'needle', 'other'), '_',
                          CAST(number AS STRING), REPEAT('x', 64))
            FROM numbers("number" = "${rowCount}")
        """
    }
    sql """
        INSERT INTO bitmap_allowlist_index_fact
        SELECT org_id, resource_id, payload FROM bitmap_allowlist_plain_fact
    """
    sql """
        INSERT INTO bitmap_allowlist_index_ids
        SELECT 4294967296 + number,
               CASE WHEN number % 80 = 0 THEN 250000
                    WHEN number % 20 = 0 THEN 1000000
                    WHEN number % 4 = 0 THEN 5000000
                    ELSE 10000000 END
        FROM numbers("number" = "20000000")
        WHERE number % 2 = 0
    """

    for (String factTable : ["bitmap_allowlist_plain_fact", "bitmap_allowlist_index_fact"]) {
        explain {
            sql "SELECT COUNT(*) FROM ${factTable} WHERE org_id = 101"
            contains "partitions=1/2"
        }
        test {
            sql "SELECT COUNT(*) AS n FROM ${factTable} WHERE org_id = 101 HAVING n <> 20000000"
            rowNum 0
        }
        test {
            sql """
                SELECT COUNT(*) AS n FROM ${factTable}
                WHERE org_id = 101 AND resource_id = 4294967393
                HAVING n <> 1
            """
            rowNum 0
        }
    }

    // Positive control: the indexed table must actually use its index for a selective lookup.
    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_index_fact
            WHERE org_id = 101
              AND bitmap_contains(bitmap_or(to_bitmap(4294967393), to_bitmap(4294967394)), resource_id)
            HAVING n <> 2
        """
        rowNum 0
    }
    sql "SET enable_profile = true"
    sql "SET profile_level = 2"
    String pointToken = "bitmap_allowlist_index_point_${UUID.randomUUID()}"
    sql """
        /* ${pointToken} */ SELECT COUNT(*) FROM bitmap_allowlist_index_fact
        WHERE org_id = 101 AND resource_id = 4294967393
    """
    String pointProfile = new ProfileAction(context).getProfileBySql(pointToken,
            ["RowsInvertedIndexFiltered"])
    def matches = Pattern.compile("RowsInvertedIndexFiltered:\\s*[^\\n]*?\\((\\d+)\\)")
            .matcher(pointProfile)
    boolean indexPruned = false
    while (matches.find()) {
        indexPruned |= Long.parseLong(matches.group(1)) > 0
    }
    assertTrue(indexPruned, "resource_id inverted index did not prune the point lookup")
    sql "SET enable_profile = false"

    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_index_fact
            WHERE org_id = 101 AND bitmap_contains(bitmap_empty(), resource_id)
            HAVING n <> 0
        """
        rowNum 0
    }
    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_nullable_fact
            WHERE org_id = 101
              AND bitmap_contains(to_bitmap(4294967393), resource_id)
              AND payload LIKE '%needle%'
            HAVING n <> 2
        """
        rowNum 0
    }
    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_nullable_fact
            WHERE org_id = 101 AND bitmap_contains(to_bitmap(97), resource_id)
            HAVING n <> 1
        """
        rowNum 0
    }
    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_nullable_fact
            WHERE org_id = 101 AND bitmap_contains(to_bitmap(99), resource_id)
            HAVING n <> 0
        """
        rowNum 0
    }
    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_index_fact
            WHERE org_id = 101 AND bitmap_contains(to_bitmap(4294967393), resource_id)
            HAVING n <> 1
        """
        rowNum 0
    }
    sql "SET enable_profile = true"
    String bitmapPointToken = "bitmap_allowlist_index_bitmap_point_${UUID.randomUUID()}"
    sql """
        /* ${bitmapPointToken} */ SELECT COUNT(*) FROM bitmap_allowlist_index_fact
        WHERE org_id = 101
          AND bitmap_contains(bitmap_or(to_bitmap(4294967393), to_bitmap(4294967394)), resource_id)
    """
    String bitmapPointProfile = new ProfileAction(context).getProfileBySql(bitmapPointToken,
            ["RowsInvertedIndexFiltered", "RowsBitmapAllowlistFiltered"])
    def bitmapPointMatches = Pattern.compile("RowsInvertedIndexFiltered:\\s*[^\\n]*?\\((\\d+)\\)")
            .matcher(bitmapPointProfile)
    boolean bitmapIndexPruned = false
    while (bitmapPointMatches.find()) {
        bitmapIndexPruned |= Long.parseLong(bitmapPointMatches.group(1)) > 0
    }
    assertTrue(bitmapIndexPruned, "small bitmap allowlist did not use the resource_id index")
    sql "SET enable_profile = false"
    test {
        sql """
            SELECT COUNT(*) AS n FROM bitmap_allowlist_index_fact
            WHERE org_id = 101 AND bitmap_contains(bitmap_from_base64(NULL), resource_id)
            HAVING n <> 0
        """
        rowNum 0
    }

    String plainFact = "bitmap_allowlist_plain_fact"
    String indexedFact = "bitmap_allowlist_index_fact"
    def logSubqueryProfile = { long size, String shape, String query ->
        sql "SET enable_profile = true"
        String token = "bitmap_subquery_${shape}_${size}_${UUID.randomUUID()}"
        sql "/* ${token} */ ${query}"
        String profile = new ProfileAction(context).getProfileBySql(token, ["RowsRead"])
        logger.info("Subquery bitmap size ${size} ${shape} profile:\n" + profile.readLines()
                .findAll { line -> ["RowsRead:", "RowsExprPredInput:", "UncompressedBytesRead:",
                        "WaitForRuntimeFilter:", "PeakMemoryUsage:", "RowsBitmapAllowlistFiltered:"].any {
                    line.contains(it)
                } }.take(35).join("\n"))
        sql "SET enable_profile = false"
    }
    for (long size : [250000L, 1000000L, 5000000L, 10000000L]) {
        test {
            sql """
                SELECT COUNT(*) AS n FROM bitmap_allowlist_index_ids
                WHERE first_size <= ${size} HAVING n <> ${size}
            """
            rowNum 0
        }
        String bitmap = sql("""
            SELECT bitmap_to_base64(bitmap_agg(resource_id))
            FROM bitmap_allowlist_index_ids WHERE first_size <= ${size}
        """)[0][0]
        def bitmapQuery = { String fact -> """
            SELECT COUNT(*) AS n, SUM(resource_id) AS s FROM ${fact}
            WHERE org_id = 101
              AND bitmap_contains(bitmap_from_base64('${bitmap}'), resource_id)
              AND payload LIKE '%needle%'
        """ }
        def joinQuery = { String fact -> """
            SELECT COUNT(*) AS n, SUM(resource_id) AS s FROM ${fact}
            WHERE org_id = 101
              AND resource_id IN (SELECT resource_id FROM bitmap_allowlist_index_ids
                                  WHERE first_size <= ${size})
              AND payload LIKE '%needle%'
        """ }
        String plainBitmap = bitmapQuery(plainFact)
        String indexedBitmap = bitmapQuery(indexedFact)
        String plainJoin = joinQuery(plainFact)
        String indexedJoin = joinQuery(indexedFact)

        test {
            sql """
                SELECT p.n FROM (${plainBitmap}) p, (${indexedBitmap}) i,
                                (${plainJoin}) j, (${indexedJoin}) k
                WHERE p.n <> i.n OR p.n <> j.n OR p.n <> k.n
                   OR p.s <> i.s OR p.s <> j.s OR p.s <> k.s
            """
            rowNum 0
        }
        if (size == 250000L) {
            sql "SET enable_profile = true"
            String bitmapToken = "bitmap_allowlist_index_early_${UUID.randomUUID()}"
            sql "/* ${bitmapToken} */ ${indexedBitmap}"
            String bitmapProfile = new ProfileAction(context).getProfileBySql(bitmapToken,
                    ["RowsBitmapAllowlistFiltered"])
            def bitmapMatches = Pattern.compile("RowsBitmapAllowlistFiltered:\\s*[^\\n]*?\\((\\d+)\\)")
                    .matcher(bitmapProfile)
            boolean bitmapPruned = false
            while (bitmapMatches.find()) {
                bitmapPruned |= Long.parseLong(bitmapMatches.group(1)) > 0
            }
            assertTrue(bitmapPruned, "bitmap allowlist did not prune before row materialization")
            sql "SET enable_profile = false"
        } else if (size == 10000000L) {
            sql "SET enable_profile = true"
            String bitmapToken = "bitmap_allowlist_index_dense_${UUID.randomUUID()}"
            sql "/* ${bitmapToken} */ ${indexedBitmap}"
            String bitmapProfile = new ProfileAction(context).getProfileBySql(bitmapToken,
                    ["RowsBitmapAllowlistFiltered"])
            def bitmapMatches = Pattern.compile("RowsBitmapAllowlistFiltered:\\s*([^\\n]+)")
                    .matcher(bitmapProfile)
            boolean foundBitmapCounter = false
            while (bitmapMatches.find()) {
                foundBitmapCounter = true
                assertTrue(bitmapMatches.group(1).trim() == "0",
                        "dense bitmap allowlist should use the normal scan")
            }
            assertTrue(foundBitmapCounter, "bitmap allowlist profile counter is missing")
            sql "SET enable_profile = false"
        }
        logger.info("Index allowlist size ${size}: SQL 1/8 = plain bitmap; SQL 2/7 = plain IN; "
                + "SQL 3/6 = indexed IN; SQL 4/5 = indexed bitmap; "
                + "bitmap Base64 bytes = ${bitmap.length()}")
        benchmark {
            warmUp true
            executeTimes 5
            skipFailure false
            sqls([plainBitmap, plainJoin, indexedJoin, indexedBitmap,
                  indexedBitmap, indexedJoin, plainJoin, plainBitmap])
        }
        String subqueryBitmap = """
            SELECT COUNT(*) AS n, SUM(resource_id) AS s FROM bitmap_allowlist_index_fact
            WHERE org_id = 101
              AND resource_id IN (
                  SELECT bitmap_agg(resource_id) FROM bitmap_allowlist_index_ids
                  WHERE first_size <= ${size})
              AND payload LIKE '%needle%'
        """
        test {
            sql """
                SELECT b.n FROM (${subqueryBitmap}) b, (${indexedJoin}) j
                WHERE b.n <> j.n OR b.s <> j.s
            """
            rowNum 0
        }
        logger.info("Subquery bitmap size ${size}: default compressed bitmap join")
        benchmark {
            warmUp true
            executeTimes 5
            skipFailure false
            sqls([subqueryBitmap])
        }
        if (size == 250000L || size == 10000000L) {
            logSubqueryProfile(size, "default", subqueryBitmap)
        }
        sql "SET enable_bitmap_subquery_hash_join = true"
        test {
            sql """
                SELECT b.n FROM (${subqueryBitmap}) b, (${indexedJoin}) j
                WHERE b.n <> j.n OR b.s <> j.s
            """
            rowNum 0
        }
        logger.info("Subquery bitmap size ${size}: expanded hash semi join")
        benchmark {
            warmUp true
            executeTimes 5
            skipFailure false
            sqls([subqueryBitmap])
        }
        if (size == 250000L || size == 10000000L) {
            logSubqueryProfile(size, "expanded", subqueryBitmap)
        }
        sql "SET enable_bitmap_subquery_hash_join = false"
    }
}
