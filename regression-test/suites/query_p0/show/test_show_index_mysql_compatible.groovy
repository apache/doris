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

suite("test_show_index_mysql_compatible") {
    def dbName = "test_show_index_mysql_compat"
    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql """
        CREATE TABLE uniq_mor (
            `user_id` LARGEINT NOT NULL,
            `username` VARCHAR(50) NOT NULL,
            `city` VARCHAR(20)
        )
        UNIQUE KEY(`user_id`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "false")
    """

    sql """
        CREATE TABLE uniq_mow (
            `user_id` LARGEINT NOT NULL,
            `event_date` DATE NOT NULL,
            `city` VARCHAR(20)
        )
        UNIQUE KEY(`user_id`, `event_date`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """

    sql """
        CREATE TABLE agg_tbl (
            `user_id` LARGEINT NOT NULL,
            `city` VARCHAR(20) NULL,
            `cost` BIGINT SUM
        )
        AGGREGATE KEY(`user_id`, `city`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE dup_tbl (
            `user_id` LARGEINT NOT NULL,
            `event_date` DATE NOT NULL,
            `note` TEXT,
            INDEX idx_note (`note`) USING INVERTED COMMENT 'note idx'
        )
        DUPLICATE KEY(`user_id`, `event_date`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // ---------------------------------------------------------------
    // Switch off: the output stays exactly what Doris has always shown.
    // ---------------------------------------------------------------
    sql "SET enable_mysql_compatible_index_metadata = false"

    // A unique key table with no secondary index reports nothing at all. This is the
    // behaviour that keeps MySQL ODBC/JDBC clients from finding a primary key, and it
    // has to stay put while the switch is off.
    assertEquals(0, sql("SHOW KEYS FROM uniq_mor").size())
    assertEquals(0, sql("SHOW KEYS FROM agg_tbl").size())

    def legacy = sql "SHOW INDEX FROM dup_tbl"
    assertEquals(1, legacy.size())
    assertEquals("idx_note", legacy[0][2])
    assertEquals("note", legacy[0][4])
    assertEquals("", legacy[0][1])   // Non_unique left blank
    assertEquals("", legacy[0][3])   // Seq_in_index left blank
    assertEquals("INVERTED", legacy[0][10])

    // ---------------------------------------------------------------
    // Switch on.
    // ---------------------------------------------------------------
    sql "SET enable_mysql_compatible_index_metadata = true"

    def mor = sql "SHOW KEYS FROM uniq_mor"
    assertEquals(1, mor.size())
    assertEquals("uniq_mor", mor[0][0])
    assertEquals("0", mor[0][1])        // Non_unique
    assertEquals("PRIMARY", mor[0][2])  // Key_name, the exact string clients match on
    assertEquals("1", mor[0][3])        // Seq_in_index
    assertEquals("user_id", mor[0][4])
    assertEquals("A", mor[0][5])        // Collation
    assertEquals("", mor[0][9])         // Null, the column is NOT NULL
    assertEquals("BTREE", mor[0][10])

    // A composite key keeps its declared order in Seq_in_index.
    def mow = sql "SHOW KEYS FROM uniq_mow"
    assertEquals(2, mow.size())
    assertEquals(["PRIMARY", "1", "user_id"], [mow[0][2], mow[0][3], mow[0][4]])
    assertEquals(["PRIMARY", "2", "event_date"], [mow[1][2], mow[1][3], mow[1][4]])

    // An aggregate key is unique as well, and a nullable key column says so.
    def agg = sql "SHOW KEYS FROM agg_tbl"
    assertEquals(2, agg.size())
    assertEquals("PRIMARY", agg[0][2])
    assertEquals("0", agg[0][1])
    assertEquals("", agg[0][9])
    assertEquals("YES", agg[1][9])

    // A duplicate key is only a sort prefix, so it is reported but never as PRIMARY.
    // Secondary indexes get one row per column.
    def dup = sql "SHOW KEYS FROM dup_tbl"
    assertEquals(3, dup.size())
    assertEquals("DUPLICATE", dup[0][2])
    assertEquals("1", dup[0][1])
    assertEquals("user_id", dup[0][4])
    assertEquals("DUPLICATE", dup[1][2])
    assertEquals("2", dup[1][3])
    assertEquals("event_date", dup[1][4])
    assertEquals("idx_note", dup[2][2])
    assertEquals("1", dup[2][1])
    assertEquals("1", dup[2][3])
    assertEquals("note", dup[2][4])
    assertEquals("INVERTED", dup[2][10])
    assertEquals("note idx", dup[2][11])

    // A declared PRIMARY KEY constraint outranks the table model, which is how the owner
    // of a duplicate key table whose data really is unique makes it usable from ODBC.
    sql "ALTER TABLE dup_tbl ADD CONSTRAINT dup_pk PRIMARY KEY (user_id)"
    def dupPk = sql "SHOW KEYS FROM dup_tbl"
    assertEquals("PRIMARY", dupPk[0][2])
    assertEquals("0", dupPk[0][1])
    assertEquals("user_id", dupPk[0][4])
    assertFalse(dupPk.any { it[2] == "DUPLICATE" })

    // A UNIQUE constraint shows up as a unique index under its own name.
    sql "ALTER TABLE uniq_mor ADD CONSTRAINT uk_username UNIQUE (username)"
    def uk = sql "SHOW KEYS FROM uniq_mor"
    assertEquals(2, uk.size())
    assertEquals("PRIMARY", uk[0][2])
    assertEquals("uk_username", uk[1][2])
    assertEquals("0", uk[1][1])
    assertEquals("username", uk[1][4])

    // KEY, KEYS, INDEX and INDEXES are the same statement.
    def byKey = sql "SHOW KEY FROM uniq_mor"
    def byIndex = sql "SHOW INDEX FROM uniq_mor"
    def byIndexes = sql "SHOW INDEXES FROM uniq_mor"
    assertEquals(uk.toString(), byKey.toString())
    assertEquals(uk.toString(), byIndex.toString())
    assertEquals(uk.toString(), byIndexes.toString())

    // The db qualified spellings the drivers use resolve to the same table.
    assertEquals(uk.toString(), sql("SHOW KEYS FROM ${dbName}.uniq_mor").toString())
    assertEquals(uk.toString(), sql("SHOW KEYS FROM uniq_mor FROM ${dbName}").toString())

    // ---------------------------------------------------------------
    // information_schema is not behind the switch: these tables report nothing today, so
    // filling them cannot change any existing behaviour.
    // ---------------------------------------------------------------
    sql "SET enable_mysql_compatible_index_metadata = false"

    def stats = sql """
        SELECT TABLE_NAME, NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION,
               SUB_PART, NULLABLE, INDEX_TYPE, IS_VISIBLE, EXPRESSION
        FROM information_schema.statistics
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mow'
        ORDER BY SEQ_IN_INDEX
    """
    assertEquals(2, stats.size())
    assertEquals(0, stats[0][1])            // NON_UNIQUE
    assertEquals("PRIMARY", stats[0][2])
    assertEquals(1, stats[0][3])            // SEQ_IN_INDEX
    assertEquals("user_id", stats[0][4])
    assertEquals("A", stats[0][5])
    assertEquals(null, stats[0][6])         // SUB_PART, Doris indexes whole columns
    assertEquals("", stats[0][7])           // NULLABLE
    assertEquals("BTREE", stats[0][8])
    assertEquals("YES", stats[0][9])
    assertEquals(null, stats[0][10])        // EXPRESSION
    assertEquals("event_date", stats[1][4])

    // This is the exact shape Connector/J asks for when useInformationSchema is on.
    def pk = sql """
        SELECT COLUMN_NAME, SEQ_IN_INDEX FROM information_schema.statistics
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mow' AND INDEX_NAME = 'PRIMARY'
        ORDER BY SEQ_IN_INDEX
    """
    assertEquals(2, pk.size())

    def constraints = sql """
        SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.table_constraints
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mor'
        ORDER BY CONSTRAINT_NAME
    """
    assertEquals(2, constraints.size())
    assertEquals(["PRIMARY", "PRIMARY KEY"], [constraints[0][0], constraints[0][1]])
    assertEquals(["uk_username", "UNIQUE"], [constraints[1][0], constraints[1][1]])

    def usage = sql """
        SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT,
               REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.key_column_usage
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mow'
        ORDER BY ORDINAL_POSITION
    """
    assertEquals(2, usage.size())
    assertEquals("PRIMARY", usage[0][0])
    assertEquals("user_id", usage[0][1])
    assertEquals(1, usage[0][2])
    assertEquals(null, usage[0][3])         // only a foreign key has a position here
    assertEquals(null, usage[0][4])
    assertEquals("event_date", usage[1][1])
    assertEquals(2, usage[1][2])

    // A foreign key fills the REFERENCED_ columns, which is what draws a relationship
    // diagram in Access and what ORMs reverse engineer. Doris only lets a foreign key
    // point at a declared PRIMARY KEY constraint, so the unique key of the table model is
    // not enough on its own here.
    sql "ALTER TABLE uniq_mor ADD CONSTRAINT pk_user_id PRIMARY KEY (user_id)"
    sql "ALTER TABLE agg_tbl ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES uniq_mor(user_id)"
    def fk = sql """
        SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT,
               REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.key_column_usage
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'agg_tbl' AND CONSTRAINT_NAME = 'fk_user'
    """
    assertEquals(1, fk.size())
    assertEquals("user_id", fk[0][1])
    assertEquals(1, fk[0][2])
    assertEquals(1, fk[0][3])
    assertEquals(dbName, fk[0][4])
    assertEquals("uniq_mor", fk[0][5])
    assertEquals("user_id", fk[0][6])

    // ---------------------------------------------------------------
    // COLUMN_KEY is behind the switch, because it already reports values today.
    // ---------------------------------------------------------------
    def columnKeys = { ->
        sql """
            SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.columns
            WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'dup_tbl'
            ORDER BY ORDINAL_POSITION
        """
    }

    sql "SET enable_mysql_compatible_index_metadata = false"
    def legacyKeys = columnKeys()
    assertEquals("DUP", legacyKeys[0][1])   // the pre-existing, non MySQL value
    assertEquals("DUP", legacyKeys[1][1])

    sql "SET enable_mysql_compatible_index_metadata = true"
    def mysqlKeys = columnKeys()
    // dup_tbl carries a declared PRIMARY KEY on user_id by now.
    assertEquals("PRI", mysqlKeys[0][1])    // user_id
    assertEquals("", mysqlKeys[1][1])       // event_date, no longer part of any key
    assertEquals("MUL", mysqlKeys[2][1])    // note, leading column of a non-unique index

    sql "SET enable_mysql_compatible_index_metadata = false"
    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
}
