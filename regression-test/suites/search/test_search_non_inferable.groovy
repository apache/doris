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

suite("test_search_non_inferable", "p0") {
    // Equal strings do not imply equal search results with different column analyzers.
    sql "DROP TABLE IF EXISTS search_non_inferable"
    sql """CREATE TABLE search_non_inferable (
            id INT,
            title VARCHAR(100),
            body VARCHAR(100),
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "unicode"),
            INDEX idx_body(body) USING INVERTED
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO search_non_inferable VALUES (1, 'hello world', 'hello world')"
    qt_match_intersect """SELECT title FROM search_non_inferable
        WHERE title MATCH 'hello'
        INTERSECT SELECT body FROM search_non_inferable ORDER BY 1"""
    qt_match_except """SELECT title FROM search_non_inferable
        WHERE title MATCH 'hello'
        EXCEPT SELECT body FROM search_non_inferable ORDER BY 1"""
    qt_multi_match_intersect """SELECT title FROM search_non_inferable
        WHERE multi_match(title, 'any', 'hello')
        INTERSECT SELECT body FROM search_non_inferable ORDER BY 1"""
    qt_multi_match_except """SELECT title FROM search_non_inferable
        WHERE multi_match(title, 'any', 'hello')
        EXCEPT SELECT body FROM search_non_inferable ORDER BY 1"""
    qt_match_join """SELECT a.id, b.id FROM search_non_inferable a
        JOIN search_non_inferable b ON a.title = b.body
        WHERE a.title MATCH 'hello' ORDER BY 1, 2"""
    qt_match_alias """SELECT id FROM (SELECT id, title AS label FROM search_non_inferable) t
        WHERE label MATCH 'hello' ORDER BY id"""
    qt_multi_match_constant """SELECT id FROM search_non_inferable
        WHERE title = 'hello world' AND multi_match(title, 'any', 'hello') ORDER BY id"""
    qt_multi_match_partial_constant """SELECT id FROM search_non_inferable
        WHERE title = 'hello world' AND multi_match(title, body, 'any', 'hello') ORDER BY id"""
    // Outer SEARCH must not acquire a different analyzer in each set-operation branch.
    ["UNION ALL", "UNION", "INTERSECT", "EXCEPT"].each { operator ->
        ["search('label:hello')", "search('label:hello') AND id > 0",
         "search('label:hello') OR id > 0", "NOT search('label:hello')"].each { predicate ->
            test {
                sql """SELECT label FROM (
                    SELECT id, title AS label FROM search_non_inferable
                    ${operator} SELECT id, body FROM search_non_inferable
                ) s WHERE ${predicate}"""
                exception "search() is only supported in filters directly on an OLAP table scan"
            }
        }
    }
    qt_search_branch_except """SELECT title FROM search_non_inferable WHERE search('title:hello')
        EXCEPT SELECT body FROM search_non_inferable ORDER BY 1"""
    qt_search_branch_union """SELECT title FROM search_non_inferable WHERE search('title:hello')
        UNION ALL SELECT body FROM search_non_inferable WHERE search('body:hello') ORDER BY 1"""
}
