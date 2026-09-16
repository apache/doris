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

suite("test_varbinary_native_paths") {
    def routing = "binary_native_routing"
    def defaults = "binary_native_defaults"
    def deletes = "binary_native_deletes"
    def keyDeletes = "binary_native_key_deletes"
    def cleanup = {
        [routing, defaults, deletes, keyDeletes].each { sql "DROP TABLE IF EXISTS ${it}" }
    }
    cleanup()
    try {
        // Equality and IN must prune to the same buckets used by the writer, without charset conversion.
        sql """CREATE TABLE ${routing} (payload VARBINARY, id INT)
               DUPLICATE KEY(payload) DISTRIBUTED BY HASH(payload) BUCKETS 7
               PROPERTIES ('replication_num'='1')"""
        def values = ["", "00", "616263", "80", "FF", "0080FF", "61626300"]
        values.eachWithIndex { value, id -> sql "INSERT INTO ${routing} VALUES (X'${value}', ${id})" }
        assertEquals(values.withIndex().collect { value, id -> [id, value] },
                sql("SELECT id, from_binary(payload) FROM ${routing} ORDER BY id"))
        values.eachWithIndex { value, id ->
            assertEquals([[id]], sql("SELECT id FROM ${routing} WHERE payload = X'${value}'"))
            assertTrue(sql("EXPLAIN SELECT id FROM ${routing} WHERE payload = X'${value}'")
                    .toString().contains("tablets=1/7"))
        }
        assertEquals([[0], [2], [4], [5]], sql("SELECT id FROM ${routing} "
                + "WHERE payload IN (X'', X'616263', X'FF', X'0080FF') ORDER BY id"))

        // Literal defaults are UTF-8 bytes; the declared VARBINARY limit counts bytes, not characters.
        sql """CREATE TABLE ${defaults} (id INT, a VARBINARY(3) DEFAULT 'abc',
               b VARBINARY(2) DEFAULT 'é', c VARBINARY(1) DEFAULT '')
               DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
               PROPERTIES ('replication_num'='1')"""
        sql "INSERT INTO ${defaults} (id) VALUES (1)"
        assertEquals([[1, "616263", "C3A9", ""]], sql("SELECT id, from_binary(a), "
                + "from_binary(b), from_binary(c) FROM ${defaults}"))

        // MOW row deletion keeps arbitrary bytes out of the text-only storage delete predicate protocol.
        sql """CREATE TABLE ${deletes} (id INT, payload VARBINARY)
               UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
               PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true',
                           'enable_mow_light_delete'='true')"""
        sql """CREATE TABLE ${keyDeletes} (payload VARBINARY, id INT)
               UNIQUE KEY(payload) DISTRIBUTED BY HASH(payload) BUCKETS 7
               PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true',
                           'enable_mow_light_delete'='true')"""
        [deletes, keyDeletes].each { table ->
            values.eachWithIndex { value, id ->
                sql "INSERT INTO ${table} (id, payload) VALUES (${id}, X'${value}')"
            }
            sql "DELETE FROM ${table} WHERE payload = X'0080FF'"
            sql "DELETE FROM ${table} WHERE payload IN (X'', X'00', X'FF')"
            assertEquals([[2, "616263"], [3, "80"], [6, "61626300"]],
                    sql("SELECT id, from_binary(payload) FROM ${table} ORDER BY id"))
        }
        test {
            sql "DELETE FROM ${routing} WHERE payload = X'0080FF'"
            exception "VARBINARY delete predicates require a Unique table with merge-on-write enabled"
        }
        assertEquals([[7L]], sql("SELECT count(*) FROM ${routing}"))
    } finally {
        cleanup()
    }
}
