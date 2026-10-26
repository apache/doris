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

// Preserve explicit wildcard fields and resolve their NULL bitmap through the logical binding.
suite("test_search_field_scoped_wildcard", "p0") {
    sql "DROP TABLE IF EXISTS test_search_field_scoped_wildcard"
    sql """
        CREATE TABLE test_search_field_scoped_wildcard (
            id INT,
            a STRING,
            b STRING,
            INDEX idx_a(a) USING INVERTED PROPERTIES("parser"="unicode"),
            INDEX idx_b(b) USING INVERTED PROPERTIES("parser"="unicode")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "inverted_index_storage_format"="V2")
    """
    sql """INSERT INTO test_search_field_scoped_wildcard VALUES
        (1, 'alpha', NULL), (2, NULL, 'beta'),
        (3, 'gamma', 'delta'), (4, NULL, NULL)"""
    sql "SYNC"
    qt_source "SELECT id, a, b FROM test_search_field_scoped_wildcard ORDER BY id"
    ["standard", "lucene"].each { mode ->
        def options = '{"mode":"' + mode + '","default_field":"a"}'
        [a: "a:*", a_group: "a:(*)", b: "b:*", b_group: "b:(*)",
         both: "a:* AND b:*", either: "a:* OR b:*",
         scoped_or: "a:(* OR nevermatchingtoken)",
         bare_star: "* OR b:nevermatchingtoken"].each { label, dsl ->
            quickTest("${mode}_${label}", """
                SELECT id FROM test_search_field_scoped_wildcard
                WHERE SEARCH('${dsl}', '${options}') ORDER BY id
            """)
        }
        quickTest("${mode}_scored", """
            SELECT id FROM (
                SELECT id, score() AS s FROM test_search_field_scoped_wildcard
                WHERE SEARCH('a:*', '${options}') ORDER BY s DESC LIMIT 20
            ) scored ORDER BY id
        """)
    }
}
