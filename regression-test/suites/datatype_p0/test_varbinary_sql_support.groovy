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
        // Exercise binary transport through views without requiring binary predicates or hash keys.
        sql "CREATE VIEW ${view} AS SELECT id, payload FROM ${table} WHERE id <> 6"
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

        // Long execution values must outlive source batches and retain their bytes.
        sql """INSERT INTO ${source} SELECT number + 100,
               concat(repeat('FF', 700), '00AB') FROM numbers('number'='2048')"""
        def longBytes = sql "SELECT id, from_binary(payload) FROM ${table} WHERE id >= 100 ORDER BY id"
        assertEquals(2048, longBytes.size())
        longBytes.eachWithIndex { row, index ->
            assertEquals([index + 100, "FF" * 700 + "00AB"], row)
        }
    } finally {
        cleanup()
    }
}
