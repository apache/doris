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

// Proves the token_bloom_filter inverted-index property is accepted by FE, persisted (visible in
// SHOW CREATE TABLE -> stored in the catalog and shipped to BE in the index meta), and that the
// resulting table is functional. Also proves an invalid value is rejected by FE validation.
suite("test_inverted_index_token_bloom_filter", "p0") {
    // Drop before use (preserve env for debugging; never drop at the end).
    sql "DROP TABLE IF EXISTS test_inverted_index_token_bloom_filter"
    sql """
        CREATE TABLE test_inverted_index_token_bloom_filter (
            id INT,
            msg STRING,
            INDEX idx_msg (msg) USING INVERTED
                PROPERTIES("parser" = "english", "support_phrase" = "true", "token_bloom_filter" = "true")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // Persisted: the property round-trips through SHOW CREATE TABLE, i.e. it is stored in the
    // catalog (and thus serialized into the index meta sent to BE), not silently dropped.
    def ddl = (sql "SHOW CREATE TABLE test_inverted_index_token_bloom_filter")
            .collect { it[1].toString().toLowerCase() }.join("\n")
    assertTrue(ddl.contains("token_bloom_filter"),
            "token_bloom_filter must persist in SHOW CREATE TABLE, got:\n" + ddl)
    assertTrue(ddl.replaceAll("\\s", "").contains("\"token_bloom_filter\"=\"true\""),
            "token_bloom_filter value must persist as true, got:\n" + ddl)

    // Functional end-to-end: the index (with the property) builds on BE and MATCH queries are
    // correct for present and absent terms.
    sql """INSERT INTO test_inverted_index_token_bloom_filter VALUES
            (1, 'apple banana'), (2, 'cherry date'), (3, 'apple cherry')"""
    sql "sync"

    def present = sql """
        SELECT id FROM test_inverted_index_token_bloom_filter WHERE msg MATCH 'apple' ORDER BY id"""
    assertEquals(2, present.size())
    assertEquals("1,3", present.collect { it[0].toString() }.join(","))

    def absent = sql """
        SELECT id FROM test_inverted_index_token_bloom_filter WHERE msg MATCH 'durianzzz' ORDER BY id"""
    assertEquals(0, absent.size())

    // Rejected: an invalid (non-boolean) value must fail FE validation.
    sql "DROP TABLE IF EXISTS test_inverted_index_token_bloom_filter_bad"
    test {
        sql """
            CREATE TABLE test_inverted_index_token_bloom_filter_bad (
                id INT,
                msg STRING,
                INDEX idx_msg (msg) USING INVERTED
                    PROPERTIES("parser" = "english", "token_bloom_filter" = "maybe")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """
        exception "token_bloom_filter must be true or false"
    }
}
