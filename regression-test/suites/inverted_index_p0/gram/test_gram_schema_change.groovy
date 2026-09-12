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

// A gram index has to survive every schema change a table can go through, and until now
// nothing checked that it does. The changes reach the index in three different ways:
//
//   - a light schema change (ADD/DROP COLUMN, a wider VARCHAR) touches no data, so the
//     existing rowsets and their index files are carried over as they are;
//   - a change that rewrites the data (a wider value or key type, a column reorder, VARCHAR
//     to STRING on the indexed column itself) writes new segments, and each of them builds a
//     fresh gram index from the converted column: it re-solves the density from its own rows,
//     recomputes the df statistics and the high-df digest, and decides stop-gram anew, so the
//     rebuilt index is not a copy of its input and cannot be assumed to behave like it;
//   - a load that lands after a change writes under the new schema.
//
// What has to hold across all of them is the same: LIKE/REGEXP through the index return
// exactly what a scan returns, and the index is still there and still prunes rows afterwards
// rather than having been silently dropped or left unreadable. The profile proves the second
// half -- RowsGramIndexFiltered > 0 is only possible if the index was read and used.
suite("test_gram_schema_change", "p0") {
    def waitAnalyzerInstalled = { String name ->
        def deadline = System.currentTimeMillis() + 180_000
        Exception lastNotFound = null
        while (System.currentTimeMillis() < deadline) {
            try {
                sql """SELECT TOKENIZE('probe', '"analyzer"="${name}"')"""
                return
            } catch (Exception e) {
                if (!e.message.contains("Policy not found")) {
                    throw e
                }
                lastNotFound = e
                sleep(1000)
            }
        }
        throw new IllegalStateException("analyzer ${name} was not installed on BE", lastNotFound)
    }

    def tableName = "test_gram_schema_change"

    // The table goes first: a policy still referenced by a table left behind by an earlier run
    // cannot be dropped.
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_sc_ana"
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_sc_tok"
    sql """CREATE INVERTED INDEX TOKENIZER gram_sc_tok PROPERTIES (
        "type"="ngram", "mode"="sparse", "min_gram"="3", "max_gram"="8", "density"="0.5")"""
    sql """CREATE INVERTED INDEX ANALYZER gram_sc_ana
        PROPERTIES ("tokenizer"="gram_sc_tok")"""
    waitAnalyzerInstalled("gram_sc_ana")

    // Two key columns, so that a key can be widened without touching the distribution column
    // (FE refuses to modify that one); a value column to widen; and the indexed column as a
    // bounded VARCHAR, so that both the length and the type of the indexed column itself can
    // change later on.
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `k2` int NULL,
        `v` int NULL,
        `msg` varchar(256) NULL,
        INDEX idx_msg (`msg`) USING INVERTED
            PROPERTIES('analyzer'='gram_sc_ana', 'support_phrase'='false')
    ) ENGINE=OLAP DUPLICATE KEY(`id`, `k2`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES('replication_num'='1', 'inverted_index_storage_format'='SNII',
               'disable_auto_compaction'='true')"""

    sql "SET enable_sql_cache=false"
    sql "SET enable_condition_cache=false"

    // Row content spans the selectivity range: a marker on every row, one on a seventh of them,
    // and a long needle on four rows out of the 5,600 the table ends up with. The needle is long
    // enough for a sparse scheme to cut at least one gram out of it whatever density the segment
    // settles on, and rare enough to keep its posting lists however the rows are laid out: a
    // gram present in more than 0.15% of a segment's rows loses its posting list to stop-gram
    // (df 6 in a 5,600-row segment) and then prunes nothing, which would read here exactly like
    // an index the schema change had dropped. Auto compaction is off for the same reason -- the
    // segment layout must change only when a schema change rewrites the data. Columns are named
    // in the INSERT so that the same loader works before and after a column is added.
    def perBatch = 800
    def loadBatch = { int b ->
        def values = []
        for (int i = 0; i < perBatch; i++) {
            def id = b * perBatch + i
            def needle = (id % 1400 == 0) ? " unique_needle_in_haystack_${id}" : ""
            def mid = (id % 7 == 0) ? " midfreq_token" : ""
            values.add("(${id}, ${id % 97}, ${id}, 'shared_prefix_common_text filler_${id}${mid}${needle}')")
        }
        sql "INSERT INTO ${tableName} (id, k2, v, msg) VALUES ${values.join(',')}"
    }
    for (int b = 0; b < 5; b++) {
        loadBatch(b)
    }

    def patterns = [
        "like_needle"   : "SELECT COUNT(*) FROM ${tableName} WHERE msg LIKE '%unique_needle_in_haystack%'",
        "like_mid"      : "SELECT COUNT(*) FROM ${tableName} WHERE msg LIKE '%midfreq_token%'",
        "like_common"   : "SELECT COUNT(*) FROM ${tableName} WHERE msg LIKE '%shared_prefix_common%'",
        "regexp_needle" : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'unique_needle_in_haystack_[0-9]+'",
        "regexp_mid"    : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'midfreq_[a-z]+'",
        "regexp_absent" : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'no_such_token_anywhere'",
        "regexp_alt"    : "SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'unique_needle_in_haystack_(0|1400|2800)\\\\b'",
        "regexp_rows"   : "SELECT id, v FROM ${tableName} WHERE msg REGEXP 'unique_needle_in_haystack_[0-9]+' ORDER BY id",
    ]

    def runAll = { ->
        def out = [:]
        patterns.each { name, stmt -> out[name] = sql(stmt) }
        return out
    }

    // Ground truth is taken fresh at every step, with the index taken out of the picture
    // entirely, because loads between the steps change the answers.
    def assertParity = { String label ->
        sql "SET enable_inverted_index_query=false"
        def scanned = runAll()
        sql "SET enable_inverted_index_query=true"
        def indexed = runAll()
        scanned.each { name, value ->
            assertEquals(value, indexed[name], "[${label}] index and scan disagree on ${name}")
        }
        assertTrue((scanned["like_needle"][0][0] as long) > 0, "[${label}] the needle rows are gone")
        logger.info("[${label}] parity held for ${patterns.size()} queries; needle rows: ${scanned['regexp_rows']}")
    }

    // A counter may be rendered as "18" or as "12.0K (12000)", and both forms must be parseable.
    def parseProfileCounter = { String profileString, String name ->
        def exact = Pattern.compile(Pattern.quote(name) + ":\\s*[^\\(\\n]*\\((\\d+)\\)").matcher(profileString)
        if (exact.find()) {
            return Long.parseLong(exact.group(1))
        }
        def plain = Pattern.compile(Pattern.quote(name) + ":\\s*(\\d+)").matcher(profileString)
        assertTrue(plain.find(), "${name} is not parseable from profile")
        return Long.parseLong(plain.group(1))
    }
    def gramProfileCounters = ["RowsGramIndexFiltered", "GramIndexCandidateRows"]
    def profileAction = new ProfileAction(context)
    // The profile is reported asynchronously by FE; ProfileAction#getProfileBySql polls until
    // this SQL's profile is complete and both gram counters have been rendered.
    def assertIndexPrunes = { String label ->
        def tag = "gram_sc_profile_${label}"
        sql "SET enable_inverted_index_query=true"
        sql "SET enable_profile=true"
        sql "SET profile_level=2"
        sql "/* ${tag} */ SELECT COUNT(*) FROM ${tableName} WHERE msg REGEXP 'unique_needle_in_haystack_[0-9]+'"
        def profileString = profileAction.getProfileBySql(tag, gramProfileCounters)
        sql "SET enable_profile=false"
        def filtered = parseProfileCounter(profileString, "RowsGramIndexFiltered")
        def candidate = parseProfileCounter(profileString, "GramIndexCandidateRows")
        logger.info("[${label}] RowsGramIndexFiltered=${filtered}, GramIndexCandidateRows=${candidate}".toString())
        assertTrue(filtered > 0, "[${label}] the gram index pruned nothing (RowsGramIndexFiltered=${filtered}): "
                + "it was dropped by the schema change or is no longer readable")
    }

    def latestAlterJob = { ->
        def rows = sql_return_maparray """SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}'
                                          ORDER BY CreateTime DESC LIMIT 1"""
        return rows.isEmpty() ? null : rows[0]
    }
    // Waits for the job this statement creates, identified by not being the job that was the
    // latest one before it; a light schema change finishes synchronously, so its job is already
    // FINISHED on the first look.
    def alterAndWait = { String label, String stmt ->
        def previous = latestAlterJob()
        def previousId = previous == null ? null : previous.JobId
        sql stmt
        def deadline = System.currentTimeMillis() + 600_000
        def job = null
        while (true) {
            job = latestAlterJob()
            if (job != null && job.JobId != previousId) {
                if (job.State == "FINISHED") {
                    break
                }
                if (job.State == "CANCELLED") {
                    throw new IllegalStateException("[${label}] schema change was cancelled: ${job.Msg}")
                }
            }
            assertTrue(System.currentTimeMillis() < deadline,
                    "[${label}] schema change did not finish within 600 s, last job: ${job}")
            sleep(1000)
        }
        logger.info("[${label}] finished: ${stmt}".toString())
    }

    def indexBytes = { ->
        def deadline = System.currentTimeMillis() + 180_000
        long bytes = 0L
        while (System.currentTimeMillis() < deadline) {
            def rows = sql """SELECT INDEX_LENGTH FROM information_schema.tables
                              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '${tableName}'"""
            bytes = rows.isEmpty() ? 0L : (rows[0][0] as long)
            if (bytes > 0) {
                break
            }
            sleep(2000)
        }
        return bytes
    }

    assertParity("base")
    assertIndexPrunes("base")
    def bytesBefore = indexBytes()
    logger.info("index bytes before any schema change: ${bytesBefore}")
    assertTrue(bytesBefore > 0, "the gram index must occupy bytes before any schema change")

    // Light: a new column, then a load that lands under the new schema.
    alterAndWait("add_column", "ALTER TABLE ${tableName} ADD COLUMN extra INT DEFAULT \"0\"")
    assertParity("add_column")
    assertIndexPrunes("add_column")
    loadBatch(5)
    assertParity("load_after_add_column")
    assertIndexPrunes("load_after_add_column")

    // Rewrites: a wider value type, then a wider key type (the sorting path).
    alterAndWait("widen_value", "ALTER TABLE ${tableName} MODIFY COLUMN v BIGINT")
    assertParity("widen_value")
    assertIndexPrunes("widen_value")
    alterAndWait("widen_key", "ALTER TABLE ${tableName} MODIFY COLUMN k2 BIGINT KEY")
    assertParity("widen_key")
    assertIndexPrunes("widen_key")

    // The indexed column itself: a longer VARCHAR, then VARCHAR to STRING.
    alterAndWait("widen_indexed_varchar", "ALTER TABLE ${tableName} MODIFY COLUMN msg VARCHAR(1024)")
    assertParity("widen_indexed_varchar")
    assertIndexPrunes("widen_indexed_varchar")
    alterAndWait("indexed_varchar_to_string", "ALTER TABLE ${tableName} MODIFY COLUMN msg STRING")
    assertParity("indexed_varchar_to_string")
    assertIndexPrunes("indexed_varchar_to_string")

    // Light again: drop the added column; then a rewrite that only reorders the columns.
    alterAndWait("drop_column", "ALTER TABLE ${tableName} DROP COLUMN extra")
    assertParity("drop_column")
    assertIndexPrunes("drop_column")
    alterAndWait("reorder", "ALTER TABLE ${tableName} ORDER BY (id, k2, msg, v)")
    assertParity("reorder")
    assertIndexPrunes("reorder")

    // A load under the final schema, and the rebuilt index still occupies bytes.
    loadBatch(6)
    assertParity("load_after_all")
    assertIndexPrunes("load_after_all")
    def bytesAfter = indexBytes()
    logger.info("index bytes after all schema changes: ${bytesAfter}")
    assertTrue(bytesAfter > 0, "the gram index must still occupy bytes after the schema changes")

    sql "SET enable_inverted_index_query=true"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS gram_sc_ana"
    sql "DROP INVERTED INDEX TOKENIZER IF EXISTS gram_sc_tok"
}
