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

// The BE condition cache must key SEARCH results by the DSL, not only by the bound columns.
// A residual predicate that the index cannot answer keeps the cache in use for the segment.
suite("test_search_condition_cache", "p0") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_inverted_index_query_cache = false"
    sql "SET enable_count_on_index_pushdown = false"

    def textTable = "search_condition_cache_text"
    def variantTable = "search_condition_cache_variant"

    def textIds = { String dsl ->
        def rows = sql """
            SELECT id FROM ${textTable}
            WHERE abs(d) >= 100 AND SEARCH('${dsl}', '{"default_field":"s","mode":"lucene","default_operator":"and"}')
            ORDER BY id
        """
        return rows.collect { it[0] as long }
    }
    def variantCount = { String dsl ->
        def rows = sql """
            SELECT COUNT(*) FROM ${variantTable}
            WHERE CAST(payload['datetime_7'] AS BIGINT) >= 100
              AND SEARCH('${dsl}', '{"default_field":"payload.string_8","mode":"lucene","default_operator":"and"}')
        """
        return rows[0][0] as long
    }

    def createTextTable = {
        sql "DROP TABLE IF EXISTS ${textTable}"
        sql """
            CREATE TABLE ${textTable} (
                id BIGINT NOT NULL,
                s TEXT NULL,
                d BIGINT NULL,
                INDEX idx_s (s) USING INVERTED
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "inverted_index_storage_format" = "V2")
        """
        // Rows sit in key order, so `alpha` is in the first 2048-row granule and `beta` in the second.
        sql """
            INSERT INTO ${textTable}
            SELECT number,
                   CASE number WHEN 1 THEN 'alpha' WHEN 3000 THEN 'beta' ELSE 'filler' END,
                   200
            FROM numbers("number" = "4096")
        """
        sql "sync"
    }
    def createVariantTable = {
        sql "DROP TABLE IF EXISTS ${variantTable}"
        sql """
            CREATE TABLE ${variantTable} (
                id BIGINT NOT NULL,
                payload VARIANT<'string_*':TEXT, 'datetime_*':BIGINT> NULL,
                INDEX idx_string (payload) USING INVERTED PROPERTIES("field_pattern" = "string_*"),
                INDEX idx_datetime (payload) USING INVERTED PROPERTIES("field_pattern" = "datetime_*")
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "inverted_index_storage_format" = "V2")
        """
        // The JSON cast stores objects; a bare string literal would be stored as a string value.
        sql """INSERT INTO ${variantTable} VALUES
            (1, CAST('{"string_8":"alpha","datetime_7":200}' AS JSON)),
            (2, CAST('{"string_8":"beta","datetime_7":200}' AS JSON))"""
        sql "sync"
    }

    // An empty result cached for `nomatch*` must not prune rows for later terms.
    createTextTable()
    sql "SET enable_condition_cache = true"
    assertEquals([], textIds("nomatch*"))
    assertEquals([1L], textIds("alpha*"))
    assertEquals([3000L], textIds("s:beta"))
    assertEquals([1L], textIds("s:alpha"))
    assertEquals([1L], textIds("alpha*"))

    // The granules cached for `alpha*` must not hide the granule that holds `beta`.
    createTextTable()
    assertEquals([1L], textIds("alpha*"))
    assertEquals([3000L], textIds("beta*"))
    assertEquals([], textIds("nomatch*"))
    assertEquals([1L], textIds("alpha*"))

    sql "SET enable_condition_cache = false"
    assertEquals([1L], textIds("alpha*"))
    assertEquals([3000L], textIds("beta*"))

    // A SEARCH without other predicates may cache the all-false result the index proves.
    createTextTable()
    sql "SET enable_condition_cache = true"
    def plainIds = { String dsl ->
        def rows = sql """
            SELECT id FROM ${textTable}
            WHERE SEARCH('${dsl}', '{"default_field":"s","mode":"lucene","default_operator":"and"}')
            ORDER BY id
        """
        return rows.collect { it[0] as long }
    }
    assertEquals([], plainIds("nomatch*"))
    assertEquals([1L], plainIds("alpha*"))
    assertEquals([3000L], plainIds("beta*"))

    // The same check on a VARIANT subcolumn with field-pattern indexes.
    createVariantTable()
    sql "SET enable_condition_cache = true"
    assertEquals(0L, variantCount("nomatch*"))
    assertEquals(1L, variantCount("alpha*"))
    sql "SET enable_condition_cache = false"
    assertEquals(1L, variantCount("alpha*"))
}
