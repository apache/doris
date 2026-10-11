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

suite("test_varbinary_sql_support") {
    // Drop before setup so failures leave the binary values and views available for inspection.
    sql "DROP MATERIALIZED VIEW IF EXISTS binary_sql_mv"
    sql "DROP VIEW IF EXISTS binary_sql_view"
    sql "DROP VIEW IF EXISTS binary_sql_nested"
    sql "DROP VIEW IF EXISTS binary_sql_source"
    sql "DROP TABLE IF EXISTS binary_sql_ctas"
    sql "DROP TABLE IF EXISTS binary_sql_input"

    // Decode bytes in the execution layer; OLAP storage remains an ordinary STRING column.
    sql """CREATE TABLE binary_sql_input (id INT, encoded STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
           PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO binary_sql_input VALUES
           (0, NULL), (1, ''), (2, '00'), (3, '7F'), (4, '80'),
           (5, 'AB'), (6, 'AB00'), (7, 'AB0001'), (8, 'AB'), (9, 'FF')"""
    // TO_BINARY treats empty input as NULL; a literal exercises a distinct empty binary value.
    sql """CREATE VIEW binary_sql_source AS SELECT id,
           CASE WHEN encoded = '' THEN X'' ELSE to_binary(encoded) END AS payload FROM binary_sql_input"""
    order_qt_binary_values "SELECT id, from_binary(payload) FROM binary_sql_source"
    sql "CREATE VIEW binary_sql_view AS SELECT id, payload FROM binary_sql_source WHERE id <> 6"
    order_qt_view_values "SELECT id, from_binary(payload) FROM binary_sql_view"
    order_qt_view_schema "DESC binary_sql_view"
    test {
        sql """CREATE TABLE binary_sql_ctas DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ('replication_num'='1') AS SELECT * FROM binary_sql_source"""
        exception "varbinary"
    }
    test {
        sql """CREATE MATERIALIZED VIEW binary_sql_mv BUILD DEFERRED REFRESH COMPLETE ON MANUAL
               DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')
               AS SELECT id, payload FROM binary_sql_source"""
        exception "varbinary"
    }

    // Materialization remains explicit: hexadecimal STRING storage is reversible.
    sql """CREATE TABLE binary_sql_ctas DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES ('replication_num'='1')
           AS SELECT id, from_binary(payload) AS payload_hex FROM binary_sql_source"""
    order_qt_materialized_hex "SELECT id, payload_hex FROM binary_sql_ctas"
    sql """CREATE VIEW binary_sql_nested AS SELECT id, array(payload) AS a,
           map('key', payload) AS m, named_struct('b', payload) AS s FROM binary_sql_source"""
    order_qt_nested_bytes """SELECT id, from_binary(a[1]), from_binary(m['key']), from_binary(s.b)
                             FROM binary_sql_nested"""

    // Compare every long value while keeping generated output compact across source batches.
    sql """INSERT INTO binary_sql_input SELECT number + 100,
           concat(repeat('FF', 700), '00AB') FROM numbers('number'='2048')"""
    order_qt_long_bytes """SELECT count(*), min(id), max(id),
            sum(if(from_binary(payload) = concat(repeat('FF', 700), '00AB'), 1, 0))
            FROM binary_sql_source WHERE id >= 100"""
}
