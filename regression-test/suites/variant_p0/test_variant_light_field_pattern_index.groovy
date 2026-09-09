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

suite("test_variant_light_field_pattern_index", "p0") {
    sql "SET default_variant_enable_doc_mode = false"
    sql "DROP TABLE IF EXISTS test_variant_light_field_pattern"
    sql """CREATE TABLE test_variant_light_field_pattern (
        id INT, v VARIANT<'num_a': STRING, 'num_b': INT>,
        INDEX idx_a(v) USING INVERTED PROPERTIES("field_pattern"="num_a"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO test_variant_light_field_pattern VALUES
        (1, parse_to_variant('{"num_a":"001","num_b":"002"}')),
        (2, parse_to_variant('{"num_a":"003","num_b":"004"}'))"""
    test {
        sql """ALTER TABLE test_variant_light_field_pattern ADD INDEX idx_b(v)
            USING INVERTED PROPERTIES("field_pattern"="num_b")"""
        exception "with analyzer default analyzer already exists"
    }
    test {
        sql "ALTER TABLE test_variant_light_field_pattern DROP INDEX idx_a"
        exception "Can not drop index with field pattern"
    }
    test {
        sql "BUILD INDEX idx_a ON test_variant_light_field_pattern"
        exception "because it is a variant type column"
    }
    sql "DROP TABLE IF EXISTS test_variant_light_field_pattern_empty"
    sql """CREATE TABLE test_variant_light_field_pattern_empty (id INT, v VARIANT<'num_b': INT>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    test {
        sql """ALTER TABLE test_variant_light_field_pattern_empty ADD INDEX idx_b(v)
            USING INVERTED PROPERTIES("field_pattern"="num_b")"""
        exception "Can not create index with field pattern"
    }
    test {
        sql "ALTER TABLE test_variant_light_field_pattern MODIFY COLUMN v VARIANT<'num_b': INT>"
        exception "referenced by index idx_a"
    }
    sql "DROP TABLE IF EXISTS test_variant_light_field_pattern_parser"
    sql """CREATE TABLE test_variant_light_field_pattern_parser (
        id INT, v VARIANT<'text': STRING>,
        INDEX idx_text(v) USING INVERTED PROPERTIES("field_pattern"="text", "parser"="english"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    test {
        sql "ALTER TABLE test_variant_light_field_pattern_parser MODIFY COLUMN v VARIANT<'text': INT>"
        exception "parser"
    }
    order_qt_before "SELECT id, v FROM test_variant_light_field_pattern"
    sql """ALTER TABLE test_variant_light_field_pattern MODIFY COLUMN v
        VARIANT<'num_a': INT, 'num_b': INT>"""
    sql """INSERT INTO test_variant_light_field_pattern VALUES
        (3, parse_to_variant('{"num_a":"001","num_b":"004"}')),
        (4, parse_to_variant('{"num_a":"006","num_b":"007"}'))"""
    order_qt_mixed "SELECT id, v FROM test_variant_light_field_pattern"
    trigger_and_wait_compaction("test_variant_light_field_pattern", "full")
    order_qt_compacted "SELECT id, v FROM test_variant_light_field_pattern"
    order_qt_index "SELECT id FROM test_variant_light_field_pattern WHERE CAST(v['num_a'] AS INT)=1"
    sql "SET enable_inverted_index_query = false"
    order_qt_scan "SELECT id FROM test_variant_light_field_pattern WHERE CAST(v['num_a'] AS INT)=1"
    sql "SET enable_inverted_index_query = true"
}
