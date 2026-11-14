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

suite("dotted_key_escape_table_property", "p0") {
    sql "set default_variant_enable_nested_group=false"
    sql "DROP TABLE IF EXISTS dotted_key_escape_table_true"
    sql "DROP TABLE IF EXISTS dotted_key_escape_table_false"
    sql """CREATE TABLE dotted_key_escape_table_true(k INT,padding INT DEFAULT '0',v VARIANT)
        DUPLICATE KEY(k,padding) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="true")"""
    sql """CREATE TABLE dotted_key_escape_table_false(k INT,v VARIANT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="false")"""
    quickRunTest("false_property", "SHOW CREATE TABLE dotted_key_escape_table_false", false,
            { row -> [row[1].contains('variant_escape_dots_in_keys').toString()] })
    ['dotted_key_escape_table_true','dotted_key_escape_table_false'].each { name ->
        ['true','false'].each { value ->
            test {
                sql "ALTER TABLE ${name} SET ('variant_escape_dots_in_keys'='${value}')"
                exception "only be specified when creating a table"
            }
        }
    }
    test {
        sql "ALTER TABLE dotted_key_escape_table_true ADD COLUMN bad VARIANT<PROPERTIES('variant_escape_dots_in_keys'='false')>"
        exception "is a table property"
    }
    test {
        sql "SELECT CAST('{}' AS VARIANT<PROPERTIES('variant_escape_dots_in_keys'='true')>)"
        exception "is a table property"
    }
    test {
        sql """CREATE TABLE dotted_key_escape_nested_bad(k INT,v VARIANT<PROPERTIES('variant_enable_nested_group'='true')>)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true')"""
        exception "Escaped tables do not support NestedGroup"
    }
    sql "set default_variant_enable_nested_group=true"
    test {
        sql """CREATE TABLE dotted_key_escape_nested_default(k INT,v VARIANT<PROPERTIES('variant_max_subcolumns_count'='1')>)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES('replication_num'='1','variant_escape_dots_in_keys'='true')"""
        exception "Escaped tables do not support NestedGroup"
    }
    test {
        sql "ALTER TABLE dotted_key_escape_table_true ADD COLUMN bad VARIANT<PROPERTIES('variant_max_subcolumns_count'='1')>"
        exception "Escaped tables do not support NestedGroup"
    }
    sql "set default_variant_enable_nested_group=false"
    sql """INSERT INTO dotted_key_escape_table_true(k,v) VALUES (1,parse_to_variant('{"a.b":101,"a":{"b":102}}'))"""
    sql "ALTER TABLE dotted_key_escape_table_true ADD ROLLUP dotted_key_escape_rollup(k,v)"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE ROLLUP WHERE TableName='dotted_key_escape_table_true' ORDER BY CreateTime DESC LIMIT 1"
        time 120
    }
    sql """INSERT INTO dotted_key_escape_table_true(k,v) VALUES (2,parse_to_variant('{"a.b":201,"a":{"b":202}}'))"""
    qt_rollup """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_table_true INDEX dotted_key_escape_rollup ORDER BY k"""
    sql "DROP TABLE IF EXISTS dotted_key_escape_rowstore"
    sql """CREATE TABLE dotted_key_escape_rowstore(k INT,v VARIANT)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="true",
            "enable_unique_key_merge_on_write"="true","store_row_column"="true")"""
    sql "INSERT INTO dotted_key_escape_rowstore SELECT k,v FROM dotted_key_escape_table_true"
    sql "set enable_two_phase_read_opt=true"
    explain {
        sql "SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_rowstore ORDER BY k LIMIT 2"
        contains "VMaterializeNode"
    }
    qt_rowstore_on """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_rowstore ORDER BY k LIMIT 2"""
    sql "set enable_two_phase_read_opt=false"
    qt_rowstore_off """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_rowstore ORDER BY k LIMIT 2"""
}
