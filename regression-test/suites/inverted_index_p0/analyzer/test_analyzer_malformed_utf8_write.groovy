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

// A string column may hold bytes that are not valid UTF-8. Indexing them must keep working:
// the malformed bytes are skipped and the valid text around them stays searchable.
suite("test_analyzer_malformed_utf8_write", "p0") {
    def ngramTable = "test_malformed_utf8_ngram"
    def icuTable = "test_malformed_utf8_icu"

    sql "DROP TABLE IF EXISTS ${ngramTable}"
    sql "DROP TABLE IF EXISTS ${icuTable}"

    sql """
        CREATE INVERTED INDEX TOKENIZER IF NOT EXISTS malformed_utf8_ngram_tokenizer
        PROPERTIES
        (
            "type" = "ngram",
            "min_gram" = "2",
            "max_gram" = "2"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS malformed_utf8_ngram_analyzer
        PROPERTIES
        (
            "tokenizer" = "malformed_utf8_ngram_tokenizer"
        );
    """

    sql """
        CREATE INVERTED INDEX ANALYZER IF NOT EXISTS malformed_utf8_icu_analyzer
        PROPERTIES
        (
            "tokenizer" = "icu"
        );
    """

    for (String analyzer : ["malformed_utf8_ngram_analyzer", "malformed_utf8_icu_analyzer"]) {
        Exception lastException = null
        boolean ready = false
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                sql """SELECT TOKENIZE('probe', '"analyzer"="${analyzer}"')"""
                ready = true
                break
            } catch (Exception e) {
                lastException = e
                sleep(1000)
            }
        }
        assertTrue(ready, "Analyzer ${analyzer} was not ready: ${lastException?.message}")
    }

    sql """
        CREATE TABLE ${ngramTable} (
            `id` int NOT NULL,
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("analyzer" = "malformed_utf8_ngram_analyzer")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql """
        CREATE TABLE ${icuTable} (
            `id` int NOT NULL,
            `ch` text NULL,
            INDEX idx_ch (`ch`) USING INVERTED PROPERTIES("analyzer" = "malformed_utf8_icu_analyzer")
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // An overlong encoding and a byte that can never start a sequence.
    sql """ INSERT INTO ${ngramTable} VALUES (1, CONCAT('abcd', UNHEX('C0AF'), 'efgh')) """
    sql """ INSERT INTO ${ngramTable} VALUES (2, CONCAT('wxyz', UNHEX('FF'))) """
    sql """ INSERT INTO ${ngramTable} VALUES (3, 'plain') """
    sql """ INSERT INTO ${icuTable} VALUES (1, CONCAT('alpha ', UNHEX('FF'), ' beta')) """
    sql """ INSERT INTO ${icuTable} VALUES (2, 'gamma delta') """

    sql "sync"

    // Every row was written, so the malformed bytes did not fail the index write.
    assertEquals(3, sql("SELECT COUNT(*) FROM ${ngramTable}")[0][0])
    assertEquals(2, sql("SELECT COUNT(*) FROM ${icuTable}")[0][0])

    // The valid text on both sides of the malformed bytes is still indexed.
    assertEquals(1, sql("SELECT COUNT(*) FROM ${ngramTable} WHERE ch MATCH 'ab'")[0][0])
    assertEquals(1, sql("SELECT COUNT(*) FROM ${ngramTable} WHERE ch MATCH 'ef'")[0][0])
    assertEquals(1, sql("SELECT COUNT(*) FROM ${ngramTable} WHERE ch MATCH 'wx'")[0][0])
    assertEquals(1, sql("SELECT COUNT(*) FROM ${icuTable} WHERE ch MATCH 'alpha'")[0][0])
    assertEquals(1, sql("SELECT COUNT(*) FROM ${icuTable} WHERE ch MATCH 'beta'")[0][0])
}
