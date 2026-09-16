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
    def source = "binary_sql_input"
    def table = "binary_sql_source"
    def view = "binary_sql_view"
    def ctas = "binary_sql_ctas"
    def nested = "binary_sql_nested"
    def mv = "binary_sql_mv"
    def cleanup = {
        sql "DROP MATERIALIZED VIEW IF EXISTS ${mv}"
        sql "DROP VIEW IF EXISTS ${view}"
        [nested, table].each { sql "DROP VIEW IF EXISTS ${it}" }
        [ctas, source].each { sql "DROP TABLE IF EXISTS ${it}" }
    }
    cleanup()
    try {
        // Decode bytes in the execution layer; OLAP storage remains an ordinary STRING column.
        sql """CREATE TABLE ${source} (id INT, encoded STRING)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
               PROPERTIES ('replication_num'='1')"""
        sql """INSERT INTO ${source} VALUES
               (0, NULL), (1, ''), (2, '00'), (3, '7F'), (4, '80'),
               (5, 'AB'), (6, 'AB00'), (7, 'AB0001'), (8, 'AB'), (9, 'FF')"""
        // TO_BINARY treats empty input as NULL; use a literal to exercise a distinct empty binary value.
        sql """CREATE VIEW ${table} AS SELECT id,
               CASE WHEN encoded = '' THEN X'' ELSE to_binary(encoded) END AS payload FROM ${source}"""
        def bytes = [[0, null], [1, ""], [2, "00"], [3, "7F"], [4, "80"],
                     [5, "AB"], [6, "AB00"], [7, "AB0001"], [8, "AB"], [9, "FF"]]
        def readBytes = { name -> sql "SELECT id, from_binary(payload) FROM ${name} ORDER BY id" }
        assertEquals(bytes, readBytes(table))
        assertEquals([[1], [2], [3], [4], [5], [8], [6], [7], [9]],
                sql("SELECT id FROM ${table} WHERE payload IS NOT NULL ORDER BY payload, id"))
        assertEquals([[5], [8]], sql("SELECT id FROM ${table} WHERE payload = X'AB' ORDER BY id"))
        assertEquals([[6], [7], [9]], sql("SELECT id FROM ${table} WHERE payload > X'AB' ORDER BY id"))
        assertEquals([[0]], sql("SELECT id FROM ${table} WHERE payload <=> NULL"))
        assertEquals([[1], [2]], sql("SELECT id FROM ${table} WHERE payload IN (X'', X'00', NULL) ORDER BY id"))
        assertEquals([[1]], sql("SELECT id FROM ${table} WHERE payload = ''"))
        assertEquals([[1, 0]], sql("SELECT CAST(X'616263' = 'abc' AS INT), CAST(X'AB' = 'AB' AS INT)"))
        assertEquals([[5], [6], [7], [8]], sql("SELECT id FROM ${table} "
                + "WHERE payload BETWEEN X'AB' AND X'AB0001' ORDER BY id"))
        assertEquals([[0, 1, 1]], sql("SELECT CAST(X'AB' = X'AB00' AS INT), "
                + "CAST(X'AB' < X'AB0001' AS INT), CAST(X'7F' < X'80' AS INT)"))
        assertEquals([[null, 1L], ["", 1L], ["00", 1L], ["7F", 1L], ["80", 1L],
                      ["AB", 2L], ["AB00", 1L], ["AB0001", 1L], ["FF", 1L]],
                sql("SELECT from_binary(payload), count(*) FROM ${table} GROUP BY payload ORDER BY payload"))
        assertEquals([[8L]], sql("SELECT count(DISTINCT payload) FROM ${table}"))
        assertEquals(bytes.collect { [it[0], 1L] },
                sql("SELECT id, count(*) FROM ${table} GROUP BY id, payload ORDER BY id"))
        // Leave runtime filters enabled: the planner must not instantiate text-only filters for binary joins.
        assertEquals([[11L]], sql("SELECT count(*) FROM ${table} a JOIN ${table} b ON a.payload = b.payload"))
        assertEquals([[12L]], sql("SELECT count(*) FROM ${table} a JOIN ${table} b ON a.payload <=> b.payload"))
        assertEquals([[35L]], sql("SELECT count(*) FROM ${table} a JOIN ${table} b ON a.payload < b.payload"))
        assertEquals([[9L]], sql("SELECT count(*) FROM ${table} a JOIN ${table} b "
                + "ON a.id = b.id AND a.payload = b.payload"))
        assertEquals([[9L]], sql("SELECT count(*) FROM ${table} WHERE payload IN (SELECT payload FROM ${table})"))
        assertEquals([[0L]], sql("SELECT count(*) FROM ${table} WHERE payload NOT IN (SELECT payload FROM ${table})"))
        assertEquals([["", "FF"]], sql("SELECT from_binary(min(payload)), from_binary(max(payload)) FROM ${table}"))

        // Unsupported binary collection kernels must fail in analysis, not with a BE internal error.
        def binaryArray = "array(payload, X'', X'0080FF', NULL)"
        ["array_contains(${binaryArray}, X'0080FF')", "array_position(${binaryArray}, X'0080FF')",
         "countequal(${binaryArray}, X'0080FF')", "array_distinct(${binaryArray})",
         "array_remove(${binaryArray}, X'0080FF')", "array_enumerate_uniq(${binaryArray})",
         "array_contains_all(${binaryArray}, ${binaryArray})", "arrays_overlap(${binaryArray}, ${binaryArray})",
         "array_union(${binaryArray}, ${binaryArray})", "array_except(${binaryArray}, ${binaryArray})",
         "array_intersect(${binaryArray}, ${binaryArray})", "collect_set(payload)", "collect_set(payload, 2)"].each {
            expression ->
            test {
                sql "SELECT ${expression} FROM ${table}"
                exception "does not support VARBINARY"
            }
        }
        assertEquals([[9L]], sql("SELECT array_size(collect_list(payload)) FROM ${table}"))

        sql "CREATE VIEW ${view} AS SELECT id, payload FROM ${table} WHERE payload <> X'AB00' OR payload IS NULL"
        assertEquals(bytes.findAll { it[0] != 6 }, readBytes(view))
        assertTrue(sql("DESC ${view}").find { it[0] == "payload" }[1].toLowerCase().startsWith("varbinary"))
        test {
            sql """CREATE TABLE ${ctas} DISTRIBUTED BY HASH(id) BUCKETS 1
                   PROPERTIES ('replication_num'='1') AS SELECT * FROM ${table}"""
            exception "varbinary"
        }
        test {
            sql """CREATE MATERIALIZED VIEW ${mv} BUILD DEFERRED REFRESH COMPLETE ON MANUAL
                   DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')
                   AS SELECT id, payload FROM ${table}"""
            exception "varbinary"
        }

        // Materialization is explicit: hexadecimal STRING storage is reversible without a new OLAP type.
        sql """CREATE TABLE ${ctas} DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ('replication_num'='1')
               AS SELECT id, from_binary(payload) AS payload_hex FROM ${table}"""
        assertEquals(bytes, sql("SELECT id, payload_hex FROM ${ctas} ORDER BY id"))

        sql """CREATE VIEW ${nested} AS SELECT id, array(payload) AS a,
               map('key', payload) AS m, named_struct('b', payload) AS s FROM ${table}"""
        assertEquals(sql("SELECT id, from_binary(payload), from_binary(payload), from_binary(payload) "
                + "FROM ${table} ORDER BY id"),
                sql("SELECT id, from_binary(a[1]), from_binary(m['key']), from_binary(s.b) "
                + "FROM ${nested} ORDER BY id"))

        // Long execution values must outlive source batches and retain their byte-exact hash keys.
        sql """INSERT INTO ${source} SELECT number + 100,
               concat(repeat('FF', 700), '00AB') FROM numbers('number'='2048')"""
        assertEquals([[2048L]], sql("SELECT count(*) FROM ${table} "
                + "WHERE payload = to_binary(concat(repeat('FF', 700), '00AB'))"))
        assertEquals([["", "FF" * 700 + "00AB"]],
                sql("SELECT from_binary(min(payload)), from_binary(max(payload)) FROM ${table}"))
    } finally {
        cleanup()
    }
}
