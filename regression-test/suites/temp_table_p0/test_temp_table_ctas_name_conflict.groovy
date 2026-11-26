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

// A temporary table is stored under <sessionId>#TEMP#<name>, so its name only has to be unique
// inside the creating session. CTAS probes the catalog for an existing table before it creates
// anything (CreateTableCommand.targetTableExists); that probe has to use the same namespace the
// table will be created in, otherwise an unrelated normal table of the same name makes
// CREATE TEMPORARY TABLE ... AS SELECT fail with "Table 'x' already exists".
suite('test_temp_table_ctas_name_conflict', 'p0') {
    def srcTable = "t_ctas_src"
    def sharedName = "t_ctas_shadowed"

    sql """DROP TABLE IF EXISTS ${srcTable}"""
    sql """
        CREATE TABLE ${srcTable} (
            id INT,
            name VARCHAR(32)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """INSERT INTO ${srcTable} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carl')"""

    // A normal table occupies the name in this database, and stays empty on purpose so that a
    // query can tell the two tables apart.
    sql """DROP TABLE IF EXISTS ${sharedName}"""
    sql """
        CREATE TABLE ${sharedName} (
            id INT,
            name VARCHAR(32)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    try {
        // The regression: this must not be rejected because of the normal table above.
        sql """
            CREATE TEMPORARY TABLE ${sharedName} PROPERTIES ('replication_num' = '1') AS
            SELECT * FROM ${srcTable}
        """

        // Inside this session the temporary table shadows the normal one.
        def showCreate = sql "show create table ${sharedName}"
        assertEquals(1, showCreate.size())
        assertEquals(sharedName, showCreate[0][0])
        assertTrue(showCreate[0][1].contains("CREATE TEMPORARY TABLE"), showCreate[0][1])
        assertEquals(3, sql("select * from ${sharedName}").size())

        // Creating it a second time in the same session does collide, because now the probe and
        // the create target the very same <sessionId>#TEMP#<name>.
        try {
            sql """
                CREATE TEMPORARY TABLE ${sharedName} PROPERTIES ('replication_num' = '1') AS
                SELECT * FROM ${srcTable}
            """
            throw new IllegalStateException("Should throw error")
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("already exists"), ex.getMessage())
        }

        // A normal table of an already taken name must still be rejected: the probe is only
        // relaxed for the temporary namespace, not disabled.
        try {
            sql """CREATE TABLE ${sharedName} PROPERTIES ('replication_num' = '1') AS SELECT * FROM ${srcTable}"""
            throw new IllegalStateException("Should throw error")
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("already exists"), ex.getMessage())
        }

        // Another session has its own temporary namespace, so it may create the same name again,
        // and it keeps seeing the normal (empty) table until it does.
        connect('root') {
            sql "use ${context.dbName}"

            def otherShowCreate = sql "show create table ${sharedName}"
            assertEquals(1, otherShowCreate.size())
            assertFalse(otherShowCreate[0][1].contains("CREATE TEMPORARY TABLE"), otherShowCreate[0][1])
            assertEquals(0, sql("select * from ${sharedName}").size())

            sql """
                CREATE TEMPORARY TABLE ${sharedName} PROPERTIES ('replication_num' = '1') AS
                SELECT * FROM ${srcTable}
            """
            assertEquals(3, sql("select * from ${sharedName}").size())
        }
    } finally {
        sql """DROP TEMPORARY TABLE IF EXISTS ${sharedName}"""
        sql """DROP TABLE IF EXISTS ${sharedName}"""
        sql """DROP TABLE IF EXISTS ${srcTable}"""
    }
}
