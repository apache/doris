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

suite("test_search_input_binding", "p0") {
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET default_variant_enable_typed_paths_to_sparse = false"
    sql "DROP TABLE IF EXISTS search_input_left"
    sql "DROP TABLE IF EXISTS search_input_right"
    sql """
        CREATE TABLE search_input_left (
            id INT,
            title VARCHAR(100),
            other_title VARCHAR(100),
            plain_title VARCHAR(100),
            v VARIANT,
            INDEX idx_title(title) USING INVERTED,
            INDEX idx_other_title(other_title) USING INVERTED,
            INDEX idx_v(v) USING INVERTED PROPERTIES("parser" = "unicode")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE search_input_right (
            id INT,
            body VARCHAR(100),
            plain_body VARCHAR(100),
            payload VARIANT,
            INDEX idx_body(body) USING INVERTED,
            INDEX idx_payload(payload) USING INVERTED PROPERTIES("parser" = "unicode")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO search_input_left VALUES
        (1, 'hello', 'world', 'hello', parse_to_variant('{"city":"beijing"}')),
        (2, 'world', 'hello', 'world', parse_to_variant('{"city":"shanghai"}')),
        (3, 'hello', 'world', 'hello', parse_to_variant('{"country":"china"}'))
    """
    sql """INSERT INTO search_input_right VALUES
        (4, 'hello', 'hello', parse_to_variant('{"city":"beijing"}')),
        (5, 'world', 'world', parse_to_variant('{"city":"shanghai"}')),
        (6, 'hello', 'hello', parse_to_variant('{"city":"beijing"}'))
    """

    explain {
        sql """
            SELECT id FROM (SELECT id, title AS label FROM search_input_left) t
            WHERE search('LABEL:hello') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
    }
    // The visible alias shadows a different indexed column in the base table.
    explain {
        sql """
            SELECT id FROM (SELECT id, title AS other_title FROM search_input_left) t
            WHERE search('other_title:hello') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
    }
    explain {
        sql """
            SELECT id FROM (
                SELECT id, label AS renamed FROM (
                    SELECT id, title AS label FROM search_input_left
                ) t
            ) u WHERE search('renamed:hello') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
    }
    explain {
        sql """
            SELECT id FROM (SELECT id, v AS doc FROM search_input_left) t
            WHERE search('DOC.city:beijing') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
        contains "sub path: [city]"
    }
    explain {
        sql """
            SELECT id FROM (SELECT id, v['city'] AS city FROM search_input_left) t
            WHERE search('city:beijing') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
        contains "sub path: [city]"
    }
    explain {
        sql """
            SELECT id FROM (SELECT id, title, v['city'] AS city FROM search_input_left) t
            WHERE search('title:hello OR city:beijing') ORDER BY id
        """
        contains "VOlapScanNode"
        contains "search("
        contains "sub path: [city]"
    }
    // Each branch has a different physical column name for the same output.
    explain {
        sql """
            SELECT id FROM (
                SELECT id, title AS label FROM search_input_left
                UNION ALL
                SELECT id, body FROM search_input_right
            ) t WHERE search('label:hello') ORDER BY id
        """
        contains "search_input_left"
        contains "search_input_right"
        multiContains "search(", 2
    }
    explain {
        sql """
            SELECT id FROM (
                SELECT id, title AS label FROM search_input_left
                UNION
                SELECT id - 3, body FROM search_input_right
            ) t WHERE search('label:hello') ORDER BY id
        """
        contains "search_input_left"
        contains "search_input_right"
        multiContains "search(", 2
    }
    explain {
        sql """
            SELECT id FROM (
                SELECT id, v AS doc FROM search_input_left
                UNION ALL
                SELECT id, payload FROM search_input_right
            ) t WHERE search('doc.city:beijing') ORDER BY id
        """
        contains "search_input_left"
        contains "search_input_right"
        multiContains "search(", 2
        contains "sub path: [city]"
    }

    // Binding uses only visible input columns and still requires a unique name.
    test {
        sql """EXPLAIN SELECT l.id FROM search_input_left l JOIN search_input_left r ON l.id = r.id
            WHERE search('TITLE:hello')"""
        exception "must resolve to exactly one input column"
    }
    test {
        sql """EXPLAIN SELECT l.id FROM search_input_left l JOIN search_input_left r ON l.id = r.id
            WHERE search('V.city:beijing')"""
        exception "must resolve to exactly one input column"
    }
    test {
        sql """EXPLAIN SELECT id FROM (SELECT id, title AS label FROM search_input_left) t
            WHERE search('title:hello')"""
        exception "must resolve to exactly one input column"
    }
    test {
        sql """EXPLAIN SELECT id FROM (SELECT id, title AS label FROM search_input_left) t
            WHERE search('label.city:beijing')"""
        exception "is not VARIANT type"
    }
    // Final validation rejects bound fields that cannot become scan predicates.
    test {
        sql """EXPLAIN SELECT l.id FROM search_input_left l JOIN search_input_right r ON l.id = r.id
            WHERE search('title:hello AND body:world')"""
        exception "search() is only supported in filters directly on an OLAP table scan"
    }
    test {
        sql """EXPLAIN SELECT id FROM (SELECT id, concat(title, 'x') AS label FROM search_input_left) t
            WHERE search('label:hellox')"""
        exception "search() fields must reference OLAP scan columns directly"
    }
    test {
        sql """EXPLAIN SELECT id FROM (SELECT id, plain_title AS label FROM search_input_left) t
            WHERE search('label:hello')"""
        exception "Field 'plain_title' has no inverted index"
    }
    test {
        sql """EXPLAIN SELECT id FROM (
                SELECT id, title AS label FROM search_input_left
                UNION ALL
                SELECT id, plain_body FROM search_input_right
            ) t WHERE search('label:hello')"""
        exception "Field 'plain_body' has no inverted index"
    }
}
