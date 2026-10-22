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

suite("test_search_index_check_after_rewrite", "p0") {
    // SEARCH on extracted Variant paths needs the columnar storage mode.
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET default_variant_enable_typed_paths_to_sparse = false"
    sql "DROP TABLE IF EXISTS search_index_check_after_rewrite"
    sql """
        CREATE TABLE search_index_check_after_rewrite (
            id INT,
            indexed_title VARCHAR(100),
            plain_title VARCHAR(100),
            indexed_v VARIANT,
            plain_v VARIANT,
            INDEX idx_title(indexed_title) USING INVERTED,
            INDEX idx_v(indexed_v) USING INVERTED PROPERTIES("parser" = "unicode")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO search_index_check_after_rewrite VALUES
        (1, 'hello', 'hello', parse_to_variant('{"city":"beijing"}'), parse_to_variant('{"city":"beijing"}')),
        (2, 'world', 'world', parse_to_variant('{"city":"shanghai"}'), parse_to_variant('{"city":"shanghai"}')),
        (3, 'hello', 'hello', parse_to_variant('{"country":"china"}'), parse_to_variant('{"country":"china"}'))
    """

    explain {
        sql """
            SELECT id FROM search_index_check_after_rewrite
            WHERE search('INDEXED_TITLE:hello') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
    }
    explain {
        sql """
            SELECT id FROM search_index_check_after_rewrite
            WHERE search('INDEXED_V.city:beijing') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
        contains "sub path: [city]"
    }
    test {
        sql """EXPLAIN SELECT id FROM search_index_check_after_rewrite WHERE search('plain_title:hello')"""
        exception "Field 'plain_title' has no inverted index"
    }
    test {
        sql """EXPLAIN SELECT id FROM search_index_check_after_rewrite WHERE search('plain_v.city:beijing')"""
        exception "Field 'plain_v' has no inverted index"
    }
    test {
        sql """EXPLAIN SELECT id FROM search_index_check_after_rewrite
            WHERE search('indexed_title:hello AND plain_title:hello')"""
        exception "Field 'plain_title' has no inverted index"
    }
    test {
        sql """EXPLAIN WITH eligible AS (
            SELECT id FROM search_index_check_after_rewrite WHERE search('plain_v.city:beijing')
        ) SELECT * FROM eligible"""
        exception "Field 'plain_v' has no inverted index"
    }

    // A SEARCH removed by rewrite does not need an index.
    explain {
        sql """
            SELECT id FROM search_index_check_after_rewrite
            WHERE search('plain_title:hello') AND FALSE ORDER BY id
        """
        contains "VEMPTYSET"
        notContains "search("
    }
    explain {
        sql """
            SELECT id FROM search_index_check_after_rewrite
            WHERE search('plain_title:hello') OR TRUE ORDER BY id
        """
        contains "VOlapScanNode"
        notContains "search("
    }
    explain {
        sql """
            SELECT id FROM search_index_check_after_rewrite
            WHERE search('plain_v.city:beijing') AND FALSE ORDER BY id
        """
        contains "VEMPTYSET"
        notContains "search("
    }
    explain {
        sql """
            SELECT id FROM search_index_check_after_rewrite
            WHERE search('plain_v.city:beijing') OR TRUE ORDER BY id
        """
        contains "VOlapScanNode"
        notContains "search("
    }
    // Syntactic position checks still run before rewrite, even for an empty result.
    test {
        sql """EXPLAIN SELECT search('plain_title:hello') FROM search_index_check_after_rewrite WHERE FALSE"""
        exception "search() is only supported in WHERE filters"
    }
}
