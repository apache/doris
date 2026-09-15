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
    def table = "binary_sql_source"
    def view = "binary_sql_view"
    def ctas = "binary_sql_ctas"
    def binaryOnly = "binary_sql_only"
    def nested = "binary_sql_nested"
    def bounded = "binary_sql_bounded"
    def prefix = "binary_sql_prefix"
    def mv = "binary_sql_mv"
    def cleanup = {
        sql "DROP MATERIALIZED VIEW IF EXISTS ${mv}"
        sql "DROP VIEW IF EXISTS ${view}"
        [ctas, binaryOnly, nested, bounded, prefix, table].each { sql "DROP TABLE IF EXISTS ${it}" }
    }
    cleanup()
    try {
        // Keep embedded NULs, high bytes and prefixes in the stored type throughout SQL execution.
        sql """CREATE TABLE ${table} (id INT, payload VARBINARY)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
               PROPERTIES ('replication_num'='1')"""
        sql """INSERT INTO ${table} VALUES
               (0, NULL), (1, X''), (2, X'00'), (3, X'7F'), (4, X'80'),
               (5, X'AB'), (6, X'AB00'), (7, X'AB0001'), (8, X'AB'), (9, X'FF')"""
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

        sql "CREATE VIEW ${view} AS SELECT id, payload FROM ${table} WHERE payload <> X'AB00' OR payload IS NULL"
        assertEquals(bytes.findAll { it[0] != 6 }, readBytes(view))
        assertTrue(sql("DESC ${view}").find { it[0] == "payload" }[1].toLowerCase().startsWith("varbinary"))
        sql """CREATE TABLE ${ctas} DISTRIBUTED BY HASH(payload) BUCKETS 3
               PROPERTIES ('replication_num'='1') AS SELECT * FROM ${table}"""
        assertTrue(sql("DESC ${ctas}").find { it[0] == "payload" }[1].toLowerCase().startsWith("varbinary"))
        assertEquals(bytes, readBytes(ctas))
        sql """CREATE TABLE ${binaryOnly} DISTRIBUTED BY HASH(payload) BUCKETS 3
               PROPERTIES ('replication_num'='1') AS SELECT payload FROM ${table}"""
        assertEquals(sql("SELECT from_binary(payload) FROM ${table} ORDER BY payload"),
                sql("SELECT from_binary(payload) FROM ${binaryOnly} ORDER BY payload"))

        sql """CREATE MATERIALIZED VIEW ${mv} BUILD DEFERRED REFRESH COMPLETE ON MANUAL
               DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES ('replication_num'='1')
               AS SELECT id, payload FROM ${table}"""
        sql "REFRESH MATERIALIZED VIEW ${mv} COMPLETE"
        waitingMTMVTaskFinishedByMvName(mv)
        assertTrue(sql("DESC ${mv}").find { it[0] == "payload" }[1].toLowerCase().startsWith("varbinary"))
        assertEquals(bytes, readBytes(mv))

        // Exercise arena-backed values, page boundaries and all-0xff truncated zone-map upper bounds.
        sql """INSERT INTO ${table} SELECT number + 100,
               to_binary(concat(repeat('FF', 700), '00AB')) FROM numbers('number'='2048')"""
        assertEquals([[2048L]], sql("SELECT count(*) FROM ${table} "
                + "WHERE payload = to_binary(concat(repeat('FF', 700), '00AB'))"))
        assertEquals([["", "FF" * 700 + "00AB"]],
                sql("SELECT from_binary(min(payload)), from_binary(max(payload)) FROM ${table}"))
        sql "INSERT INTO ${binaryOnly} SELECT payload FROM ${table} WHERE id = 100"
        assertEquals([[2048L]], sql("SELECT count(*) FROM ${table} a JOIN ${binaryOnly} b "
                + "ON a.payload = b.payload WHERE a.id >= 100"))
        sql "REFRESH MATERIALIZED VIEW ${mv} COMPLETE"
        waitingMTMVTaskFinishedByMvName(mv)
        assertEquals([[2048L]], sql("SELECT count(*) FROM ${mv} WHERE length(payload) = 702"))

        // A carried zone-map upper bound is safe for pruning, but is not the actual MAX value.
        sql """CREATE TABLE ${prefix} DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ('replication_num'='1') AS SELECT 1 AS id,
               to_binary(concat('61', repeat('FF', 699))) AS payload"""
        assertEquals([["61" + "FF" * 699]], sql("SELECT from_binary(max(payload)) FROM ${prefix}"))

        sql """CREATE TABLE ${nested} DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ('replication_num'='1') AS SELECT id, array(payload) AS a,
               map('key', payload) AS m, named_struct('b', payload) AS s FROM ${table}"""
        assertEquals(sql("SELECT id, from_binary(payload), from_binary(payload), from_binary(payload) "
                + "FROM ${table} ORDER BY id"),
                sql("SELECT id, from_binary(a[1]), from_binary(m['key']), from_binary(s.b) "
                + "FROM ${nested} ORDER BY id"))

        // A declared byte limit must reject oversize values, including nested values, without UTF-8 truncation.
        sql "SET enable_insert_strict = true"
        sql """CREATE TABLE ${bounded} (id INT, payload VARBINARY(2), items ARRAY<VARBINARY(2)>)
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ('replication_num'='1')"""
        assertEquals("varbinary(2)", sql("DESC ${bounded}").find { it[0] == "payload" }[1].toLowerCase())
        sql "INSERT INTO ${bounded} VALUES (1, X'FF00', [X'00FF', NULL])"
        test {
            sql "INSERT INTO ${bounded} VALUES (2, X'FF0001', [X'00FF'])"
            exception "Insert has filtered data"
        }
        test {
            sql "INSERT INTO ${bounded} VALUES (3, X'FF00', [X'00FF01'])"
            exception "Insert has filtered data"
        }
        assertEquals([[1L]], sql("SELECT count(*) FROM ${bounded}"))
    } finally {
        cleanup()
    }
}
