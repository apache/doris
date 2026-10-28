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

import groovy.json.JsonOutput

suite("test_variant_projection_deferred_init", "p0") {
    sql "DROP TABLE IF EXISTS variant_projection_deferred_init"
    sql """
        CREATE TABLE variant_projection_deferred_init (
            k INT NOT NULL,
            tag STRING,
            attrs VARIANT<PROPERTIES("variant_max_subcolumns_count"="1024")>,
            INDEX idx_tag(tag) USING INVERTED,
            INDEX idx_attrs(attrs) USING INVERTED
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    def background = [a: [x: -1], other: [:]]
    (0..<512).each { background.other["k${it}"] = it }
    def backgroundJson = JsonOutput.toJson(background)
    // The tag zone map spans the target value, but the inverted index rejects
    // these rowsets without needing the wide VARIANT projection.
    (0..<3).each {
        sql """INSERT INTO variant_projection_deferred_init VALUES
               (0, '000', parse_to_variant('${backgroundJson}')), (100, 'zzz', parse_to_variant('${backgroundJson}'))"""
    }
    def first = '{"a":{"x":7,"name":"first"},"arr":[1,2]}'
    def last = '{"a":{"x":9,"name":"last"},"arr":[3,4],"flag":true}'
    sql """INSERT INTO variant_projection_deferred_init VALUES
           (1, 'match', parse_to_variant('${first}')), (2, 'match', parse_to_variant('${last}')), (3, 'null', NULL)"""

    order_qt_index_root """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init WHERE tag = 'match'
    """
    order_qt_index_empty """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init WHERE tag = 'absent'
    """
    order_qt_variant_predicate """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init
        WHERE tag = 'match' AND CAST(attrs['a']['x'] AS INT) >= 8
    """
    order_qt_expression """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init WHERE tag LIKE 'ma%'
    """
    order_qt_subtree """
        SELECT k, CAST(attrs['a'] AS STRING), attrs['arr']
        FROM variant_projection_deferred_init WHERE tag = 'match'
    """
    qt_topn """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init
        WHERE tag = 'match' ORDER BY CAST(attrs['a']['x'] AS INT) DESC LIMIT 1
    """
    order_qt_null """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init WHERE tag = 'null'
    """

    sql "SET enable_inverted_index_query = false"
    order_qt_vector_root """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init WHERE tag = 'match'
    """
    order_qt_vector_empty """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_init WHERE tag = 'absent'
    """
    sql "SET enable_inverted_index_query = true"

    sql "DROP TABLE IF EXISTS variant_projection_deferred_mow"
    sql """
        CREATE TABLE variant_projection_deferred_mow (
            k INT NOT NULL, tag STRING, attrs VARIANT,
            INDEX idx_tag(tag) USING INVERTED
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true")
    """
    sql """INSERT INTO variant_projection_deferred_mow VALUES
           (1, 'match', parse_to_variant('${first}')), (2, 'match', parse_to_variant('${last}'))"""
    sql "DELETE FROM variant_projection_deferred_mow WHERE k = 2"
    order_qt_mow_delete """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_mow WHERE tag = 'match'
    """
    sql """INSERT INTO variant_projection_deferred_mow VALUES (1, 'replaced', parse_to_variant('${last}'))"""
    order_qt_mow_empty """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_mow WHERE tag = 'match'
    """
    order_qt_mow_visible """
        SELECT k, CAST(attrs AS STRING) FROM variant_projection_deferred_mow WHERE tag = 'replaced'
    """
}
