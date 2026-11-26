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
    //
    // A unique key table with no secondary index reports nothing at all. That is the
    // behaviour that keeps MySQL ODBC/JDBC clients from finding a primary key, and it has
    // to stay put while the switch is off.
    // ---------------------------------------------------------------
    sql "SET enable_mysql_compatible_index_metadata = false"

    qt_legacy_uniq "SHOW KEYS FROM uniq_mor"
    qt_legacy_agg "SHOW KEYS FROM agg_tbl"
    qt_legacy_dup "SHOW INDEX FROM dup_tbl"

    // ---------------------------------------------------------------
    // Switch on.
    // ---------------------------------------------------------------
    sql "SET enable_mysql_compatible_index_metadata = true"

    // Key_name is PRIMARY, the exact string ODBC and JDBC match on, and Non_unique is 0.
    qt_mysql_uniq_mor "SHOW KEYS FROM uniq_mor"
    // A composite key keeps its order in Seq_in_index.
    qt_mysql_uniq_mow "SHOW KEYS FROM uniq_mow"
    // An aggregate key is unique as well, and a nullable key column says so under Null.
    qt_mysql_agg "SHOW KEYS FROM agg_tbl"
    // A duplicate key is only a sort prefix, so it is reported but never as PRIMARY.
    // A secondary index gets one row per column, and its declared description lands in
    // Index_comment the way MySQL reports it, not in Comment.
    qt_mysql_dup "SHOW KEYS FROM dup_tbl"

    // A declared PRIMARY KEY constraint outranks the table model, which is how the owner of
    // a duplicate key table whose data really is unique makes it usable from ODBC.
    sql "ALTER TABLE dup_tbl ADD CONSTRAINT dup_pk PRIMARY KEY (user_id)"
    qt_mysql_dup_declared_pk "SHOW KEYS FROM dup_tbl"

    // A UNIQUE constraint shows up as a unique index under its own name.
    sql "ALTER TABLE uniq_mor ADD CONSTRAINT uk_username UNIQUE (username)"
    qt_mysql_unique_constraint "SHOW KEYS FROM uniq_mor"

    // KEY, KEYS, INDEX and INDEXES are the same statement, and the db qualified spellings
    // the drivers use resolve to the same table.
    def expected = sql "SHOW KEYS FROM uniq_mor"
    assertEquals(expected.toString(), sql("SHOW KEY FROM uniq_mor").toString())
    assertEquals(expected.toString(), sql("SHOW INDEX FROM uniq_mor").toString())
    assertEquals(expected.toString(), sql("SHOW INDEXES FROM uniq_mor").toString())
    assertEquals(expected.toString(), sql("SHOW KEYS FROM ${dbName}.uniq_mor").toString())
    assertEquals(expected.toString(), sql("SHOW KEYS FROM uniq_mor FROM ${dbName}").toString())

    // A declared key is reported in the order it was declared, not in schema order: a
    // client reading Seq_in_index has to see the key the user actually wrote down.
    sql """
        CREATE TABLE declared_order (
            `a` INT NOT NULL,
            `b` INT NOT NULL,
            `c` INT
        )
        DUPLICATE KEY(`a`, `b`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "ALTER TABLE declared_order ADD CONSTRAINT pk_ba PRIMARY KEY (b, a)"
    qt_declared_key_order "SHOW KEYS FROM declared_order"

    // A temporary table is reported under the name the user typed. Its stored name is
    // qualified with the id of the session that owns it, which must not reach a client.
    sql """
        CREATE TEMPORARY TABLE temp_keys (
            `id` INT NOT NULL,
            `v` VARCHAR(20)
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    qt_temp_table_name "SHOW KEYS FROM temp_keys"

    // ---------------------------------------------------------------
    // information_schema is not behind the switch: these tables report nothing today, so
    // filling them cannot change any existing behaviour.
    // ---------------------------------------------------------------
    sql "SET enable_mysql_compatible_index_metadata = false"

    qt_is_statistics """
        SELECT TABLE_NAME, NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION,
               CARDINALITY, SUB_PART, PACKED, NULLABLE, INDEX_TYPE, COMMENT, INDEX_COMMENT,
               IS_VISIBLE, EXPRESSION
        FROM information_schema.statistics
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mow'
        ORDER BY SEQ_IN_INDEX
    """

    // The exact shape Connector/J asks for when useInformationSchema is on.
    qt_is_connectorj_primary_keys """
        SELECT TABLE_CATALOG AS TABLE_CAT, TABLE_SCHEMA AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,
               SEQ_IN_INDEX AS KEY_SEQ, 'PRIMARY' AS PK_NAME
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mow' AND INDEX_NAME = 'PRIMARY'
        ORDER BY SEQ_IN_INDEX
    """

    qt_is_table_constraints """
        SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM information_schema.table_constraints
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mor'
        ORDER BY CONSTRAINT_NAME
    """

    qt_is_key_column_usage """
        SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT,
               REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.key_column_usage
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mow'
        ORDER BY ORDINAL_POSITION
    """

    // A temporary table belongs to one session, so it is absent from information_schema
    // entirely -- which is also what MySQL does with one.
    qt_is_no_temp_tables """
        SELECT TABLE_NAME FROM information_schema.statistics
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'temp_keys'
    """

    // A foreign key fills the REFERENCED_ columns, which is what draws a relationship
    // diagram in Access and what ORMs reverse engineer. Doris only lets a foreign key point
    // at a declared PRIMARY KEY constraint, so the unique key of the table model is not
    // enough on its own here.
    sql "ALTER TABLE uniq_mor ADD CONSTRAINT pk_user_id PRIMARY KEY (user_id)"
    sql "ALTER TABLE agg_tbl ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES uniq_mor(user_id)"
    qt_is_foreign_key """
        SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT,
               REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.key_column_usage
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'agg_tbl' AND CONSTRAINT_NAME = 'fk_user'
        ORDER BY ORDINAL_POSITION
    """

    // POSITION_IN_UNIQUE_CONSTRAINT is where the referenced column sits in the key of the
    // parent, not where the local column sits in the foreign key. A reference written in
    // the other order than the parent key has to report 2 then 1.
    sql """
        CREATE TABLE fk_parent (
            `a` INT NOT NULL,
            `b` INT NOT NULL
        )
        DUPLICATE KEY(`a`, `b`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE fk_child (
            `x` INT NOT NULL,
            `y` INT NOT NULL
        )
        DUPLICATE KEY(`x`, `y`)
        DISTRIBUTED BY HASH(`x`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "ALTER TABLE fk_parent ADD CONSTRAINT pk_ab PRIMARY KEY (a, b)"
    sql "ALTER TABLE fk_child ADD CONSTRAINT fk_reordered FOREIGN KEY (x, y) REFERENCES fk_parent(b, a)"
    qt_is_foreign_key_reordered """
        SELECT COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT, REFERENCED_COLUMN_NAME
        FROM information_schema.key_column_usage
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'fk_child'
        ORDER BY ORDINAL_POSITION
    """

    // ---------------------------------------------------------------
    // COLUMN_KEY is behind the switch, because it already reports values today.
    // ---------------------------------------------------------------
    sql "SET enable_mysql_compatible_index_metadata = false"
    // The pre-existing, non MySQL values.
    qt_column_key_legacy """
        SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.columns
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'dup_tbl'
        ORDER BY ORDINAL_POSITION
    """

    sql "SET enable_mysql_compatible_index_metadata = true"
    // dup_tbl carries a declared PRIMARY KEY on user_id by now, so user_id is PRI, the
    // event_date that is no longer part of any key is blank, and note is MUL because it
    // leads a non-unique index.
    qt_column_key_mysql """
        SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.columns
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'dup_tbl'
        ORDER BY ORDINAL_POSITION
    """

    // The leading column of a composite UNIQUE index is not unique on its own -- the
    // combination is -- so MySQL reports it as MUL, the same as any other index prefix.
    sql "ALTER TABLE uniq_mow ADD CONSTRAINT uk_composite UNIQUE (city, event_date)"
    qt_column_key_composite_unique """
        SELECT COLUMN_NAME, COLUMN_KEY FROM information_schema.columns
        WHERE TABLE_SCHEMA = '${dbName}' AND TABLE_NAME = 'uniq_mow'
        ORDER BY ORDINAL_POSITION
    """

    // SHOW COLUMNS has to say the same thing whether or not a WHERE clause routes it
    // through information_schema.columns. Adding a predicate must not change what the Key
    // column means.
    //
    // The WHERE form pins only TABLE_NAME, not TABLE_SCHEMA, so this needs a table name no
    // other suite is likely to have created.
    sql """
        CREATE TABLE show_columns_key_check_tbl (
            `k1` INT NOT NULL,
            `k2` INT NOT NULL,
            `v` VARCHAR(20)
        )
        UNIQUE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    //
    // Sorted, because only the direct form walks the schema in order -- the WHERE form is
    // a plain scan of information_schema.columns with no ORDER BY of its own. Sorting both
    // is what makes the two blocks below comparable line by line.
    order_qt_show_columns_direct "SHOW COLUMNS FROM show_columns_key_check_tbl"
    order_qt_show_columns_where "SHOW COLUMNS FROM show_columns_key_check_tbl WHERE Field != ''"
    order_qt_show_full_columns_direct "SHOW FULL COLUMNS FROM show_columns_key_check_tbl"

    sql "SET enable_mysql_compatible_index_metadata = false"
    // The database is deliberately left behind: the fixture is dropped at the start of the
    // suite instead, so that the state of a failed run stays around to be looked at.
}
