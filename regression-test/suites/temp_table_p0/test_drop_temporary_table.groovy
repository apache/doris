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

// DROP TEMPORARY TABLE only ever targets the temporary table of the current session. Name
// resolution falls back to the normal table when this session owns no temporary one, so the drop
// path has to tell "found the temporary table" from "found something else with that name":
// it must never drop the normal table, and IF EXISTS must turn the miss into a no-op instead of
// raising "Unknown table".
suite('test_drop_temporary_table', 'p0') {
    def srcTable = "t_drop_temp_src"
    def sharedName = "t_drop_temp_shared"

    // The two tables carry different row counts so a query can tell which one it resolved to:
    // the normal table holds 1 row, the temporary table built from srcTable holds 3.
    sql """DROP TABLE IF EXISTS ${srcTable}"""
    sql """
        CREATE TABLE ${srcTable} (id INT, name VARCHAR(32))
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """INSERT INTO ${srcTable} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carl')"""

    sql """DROP TABLE IF EXISTS ${sharedName}"""
    sql """
        CREATE TABLE ${sharedName} (id INT, name VARCHAR(32))
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """INSERT INTO ${sharedName} VALUES (99, 'normal')"""

    try {
        // Nothing at all goes by this name: plainly a no-op.
        sql """DROP TEMPORARY TABLE IF EXISTS t_drop_temp_never_existed"""

        // Only a normal table goes by this name. The temporary table asked for does not exist,
        // so IF EXISTS makes this a no-op -- and the normal table must survive untouched.
        sql """DROP TEMPORARY TABLE IF EXISTS ${sharedName}"""
        assertEquals(1, sql("select * from ${sharedName}").size())

        // Without IF EXISTS the same statement reports the missing temporary table, and still
        // must not drop the normal one.
        try {
            sql """DROP TEMPORARY TABLE ${sharedName}"""
            throw new IllegalStateException("Should throw error")
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Unknown table"), ex.getMessage())
        }
        assertEquals(1, sql("select * from ${sharedName}").size())

        // With a temporary table shadowing the normal one, the drop takes the temporary table
        // and leaves the normal table in place.
        sql """
            CREATE TEMPORARY TABLE ${sharedName} PROPERTIES ('replication_num' = '1') AS
            SELECT * FROM ${srcTable}
        """
        assertEquals(3, sql("select * from ${sharedName}").size())
        sql """DROP TEMPORARY TABLE ${sharedName}"""
        assertEquals(1, sql("select * from ${sharedName}").size())

        // A temporary table belongs to the session that created it: another session neither sees
        // it nor can drop it, and its IF EXISTS no-op must leave both tables alone.
        sql """
            CREATE TEMPORARY TABLE ${sharedName} PROPERTIES ('replication_num' = '1') AS
            SELECT * FROM ${srcTable}
        """
        connect('root') {
            sql "use ${context.dbName}"
            assertEquals(1, sql("select * from ${sharedName}").size())
            sql """DROP TEMPORARY TABLE IF EXISTS ${sharedName}"""
            assertEquals(1, sql("select * from ${sharedName}").size())
        }
        assertEquals(3, sql("select * from ${sharedName}").size())
    } finally {
        sql """DROP TEMPORARY TABLE IF EXISTS ${sharedName}"""
        sql """DROP TABLE IF EXISTS ${sharedName}"""
        sql """DROP TABLE IF EXISTS ${srcTable}"""
    }
}
