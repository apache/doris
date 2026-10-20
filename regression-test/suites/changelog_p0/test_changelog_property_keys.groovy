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

// The changelog is configured by the "changelog.*" properties at both the table and the database
// level, stored under those names. The legacy keys "binlog.format" and
// "binlog.need_historical_value" are rejected in every DDL, whatever their value, and CCR binlog
// and changelog are mutually exclusive.
suite("test_changelog_property_keys") {
    if (isCloudMode()) {
        logger.info("skip test_changelog_property_keys in cloud mode")
        return
    }

    // Both switches are EXPERIMENTAL configs, so SHOW FRONTEND CONFIG may report them with the
    // experimental_ prefix.
    def featureEnabled = { String name ->
        def row = sql("SHOW FRONTEND CONFIG LIKE '%${name}'").find {
            it[0] == name || it[0] == "experimental_${name}"
        }
        return row != null && row[1] == "true"
    }
    if (!featureEnabled("enable_feature_changelog") || !featureEnabled("enable_feature_binlog")) {
        logger.info("fe enable_feature_changelog or enable_feature_binlog is false, " +
                "skip case test_changelog_property_keys")
        return
    }

    def formatRemoved = 'Property binlog.format is no longer supported, ' +
            'use "changelog.enable" = "true" to enable changelog, or remove it for CCR binlog'
    def historicalValueRemoved = 'Property binlog.need_historical_value is no longer supported, ' +
            'use changelog.need_historical_value instead'

    sql "DROP TABLE IF EXISTS test_changelog_property_keys_row_tbl FORCE"
    sql "DROP TABLE IF EXISTS test_changelog_property_keys_ccr_tbl FORCE"
    sql "DROP TABLE IF EXISTS test_changelog_property_keys_invalid_tbl FORCE"
    sql "DROP TABLE IF EXISTS test_changelog_property_keys_tmp_tbl"
    sql "DROP DATABASE IF EXISTS test_changelog_property_keys_db FORCE"
    sql "DROP DATABASE IF EXISTS test_changelog_property_keys_default_db FORCE"
    sql "DROP DATABASE IF EXISTS test_changelog_property_keys_invalid_db FORCE"
    sql "DROP DATABASE IF EXISTS test_changelog_property_keys_ccr_db FORCE"
    sql "DROP DATABASE IF EXISTS test_changelog_property_keys_recover_db FORCE"

    sql """
        CREATE TABLE test_changelog_property_keys_row_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true"
        )
    """
    sql """
        CREATE TABLE test_changelog_property_keys_ccr_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true"
        )
    """
    order_qt_table_format """
        SELECT TABLE_NAME, PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.table_properties
        WHERE TABLE_CATALOG = 'internal'
            AND TABLE_SCHEMA = '${context.dbName}'
            AND TABLE_NAME IN ('test_changelog_property_keys_row_tbl', 'test_changelog_property_keys_ccr_tbl')
            AND PROPERTY_NAME IN ('binlog.enable', 'changelog.enable')
    """

    // The table has a real changelog: the write below is readable from it.
    sql "INSERT INTO test_changelog_property_keys_row_tbl VALUES (1, 10), (2, 20)"
    order_qt_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op, k1, v1
        FROM changelog("table" = "test_changelog_property_keys_row_tbl")
    """

    // The legacy keys are rejected when a table is created, whatever the value is.
    for (def format : ["row", "ROW", "Statement_And_Snapshot", "rows"]) {
        test {
            sql """
                CREATE TABLE test_changelog_property_keys_invalid_tbl (
                    k1 INT NOT NULL,
                    v1 INT
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "binlog.enable" = "true",
                    "binlog.format" = "${format}"
                )
            """
            exception formatRemoved
        }
    }
    test {
        sql """
            CREATE TABLE test_changelog_property_keys_invalid_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "changelog.enable" = "true",
                "binlog.need_historical_value" = "true"
            )
        """
        exception historicalValueRemoved
    }

    // ... and when a table is altered.
    test {
        sql """ALTER TABLE test_changelog_property_keys_row_tbl SET ("binlog.format" = "Row")"""
        exception formatRemoved
    }
    test {
        sql """ALTER TABLE test_changelog_property_keys_ccr_tbl SET ("binlog.format" = "statement_and_snapshot")"""
        exception formatRemoved
    }
    test {
        sql """ALTER TABLE test_changelog_property_keys_row_tbl SET ("binlog.need_historical_value" = "true")"""
        exception historicalValueRemoved
    }

    // The other changelog properties need changelog to be enabled, and unknown keys or values are rejected.
    test {
        sql """
            CREATE TABLE test_changelog_property_keys_invalid_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "changelog.ttl_seconds" = "3600"
            )
        """
        exception 'Property changelog.ttl_seconds requires "changelog.enable" = "true"'
    }
    test {
        sql """
            CREATE TABLE test_changelog_property_keys_invalid_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "changelog.need_historical_value" = "true"
            )
        """
        exception 'Property changelog.need_historical_value requires "changelog.enable" = "true"'
    }
    test {
        sql """
            CREATE TABLE test_changelog_property_keys_invalid_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "changelog.enable" = "true",
                "changelog.format" = "row"
            )
        """
        exception "Unknown changelog property: changelog.format"
    }
    test {
        sql """
            CREATE TABLE test_changelog_property_keys_invalid_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "changelog.enable" = "yes"
            )
        """
        exception "Invalid changelog.enable value: yes"
    }

    // A table can not have both CCR binlog and changelog.
    test {
        sql """
            CREATE TABLE test_changelog_property_keys_invalid_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "changelog.enable" = "true"
            )
        """
        exception "Table test_changelog_property_keys_invalid_tbl can not enable both binlog and changelog"
    }

    // A temporary table can not have changelog.
    test {
        sql """
            CREATE TEMPORARY TABLE test_changelog_property_keys_tmp_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "changelog.enable" = "true"
            )
        """
        exception "Cannot create temporary table with changelog enable"
    }

    // A database stores the changelog properties under their own names.
    sql """
        CREATE DATABASE test_changelog_property_keys_db PROPERTIES (
            "changelog.enable" = "true"
        )
    """
    order_qt_db_format """
        SELECT PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.database_properties
        WHERE CATALOG_NAME = 'internal'
            AND SCHEMA_NAME = 'test_changelog_property_keys_db'
            AND PROPERTY_NAME IN ('binlog.enable', 'changelog.enable')
    """

    // A table created in the database inherits the changelog.
    sql """
        CREATE TABLE test_changelog_property_keys_db.test_changelog_property_keys_inherited_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    order_qt_inherited_table_format """
        SELECT PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.table_properties
        WHERE TABLE_CATALOG = 'internal'
            AND TABLE_SCHEMA = 'test_changelog_property_keys_db'
            AND TABLE_NAME = 'test_changelog_property_keys_inherited_tbl'
            AND PROPERTY_NAME IN ('binlog.enable', 'changelog.enable')
    """

    // In a database whose default is changelog, a temporary table created without changelog properties is a
    // plain table.
    sql """
        CREATE DATABASE test_changelog_property_keys_default_db PROPERTIES (
            "changelog.enable" = "true"
        )
    """
    sql """
        CREATE TEMPORARY TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_tmp_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    qt_temporary_table "SHOW CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_tmp_tbl"

    // A table without changelog in the database shows "changelog.enable" = "false", while a table in a
    // database without that default shows no changelog key.
    sql """
        CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_dup_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "false"
        )
    """
    qt_opt_out_table "SHOW CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_dup_tbl"
    qt_ccr_table "SHOW CREATE TABLE test_changelog_property_keys_ccr_tbl"

    // CREATE TABLE LIKE copies a table without changelog as a table without changelog.
    sql """
        CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_dup_like_tbl
        LIKE test_changelog_property_keys_default_db.test_changelog_property_keys_dup_tbl
    """
    qt_dup_like_table "SHOW CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_dup_like_tbl"
    test {
        sql """
            SELECT * FROM changelog("db" = "test_changelog_property_keys_default_db",
                "table" = "test_changelog_property_keys_dup_like_tbl")
        """
        exception "changelog is not enabled for table=test_changelog_property_keys_dup_like_tbl"
    }

    // The aggregate and merge-on-read unique models have no changelog, and LIKE copies them as well.
    sql """
        CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_agg_tbl (
            k1 INT NOT NULL,
            v1 INT SUM
        )
        AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "false"
        )
    """
    sql """
        CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_agg_like_tbl
        LIKE test_changelog_property_keys_default_db.test_changelog_property_keys_agg_tbl
    """
    qt_agg_like_table "SHOW CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_agg_like_tbl"
    sql """
        CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_mor_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false",
            "changelog.enable" = "false"
        )
    """
    sql """
        CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_mor_like_tbl
        LIKE test_changelog_property_keys_default_db.test_changelog_property_keys_mor_tbl
    """
    qt_mor_like_table "SHOW CREATE TABLE test_changelog_property_keys_default_db.test_changelog_property_keys_mor_like_tbl"

    sql """
        ALTER DATABASE test_changelog_property_keys_db SET PROPERTIES (
            "changelog.enable" = "false"
        )
    """
    order_qt_altered_db_format """
        SELECT PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.database_properties
        WHERE CATALOG_NAME = 'internal'
            AND SCHEMA_NAME = 'test_changelog_property_keys_db'
            AND PROPERTY_NAME IN ('binlog.enable', 'changelog.enable')
    """

    // The table that inherited the changelog keeps the database from turning on CCR binlog.
    test {
        sql """ALTER DATABASE test_changelog_property_keys_db SET PROPERTIES ("binlog.enable" = "true")"""
        exception "Can not enable binlog in database test_changelog_property_keys_db because table " +
                "test_changelog_property_keys_inherited_tbl enables changelog"
    }

    // The legacy keys are rejected for a database as well.
    test {
        sql """
            CREATE DATABASE test_changelog_property_keys_invalid_db PROPERTIES (
                "binlog.enable" = "true",
                "binlog.format" = "row"
            )
        """
        exception formatRemoved
    }
    test {
        sql """
            CREATE DATABASE test_changelog_property_keys_invalid_db PROPERTIES (
                "changelog.enable" = "true",
                "binlog.need_historical_value" = "true"
            )
        """
        exception historicalValueRemoved
    }
    test {
        sql """ALTER DATABASE test_changelog_property_keys_db SET PROPERTIES ("binlog.format" = "rows")"""
        exception formatRemoved
    }
    test {
        sql """ALTER DATABASE test_changelog_property_keys_db SET PROPERTIES ("binlog.need_historical_value" = "true")"""
        exception historicalValueRemoved
    }

    // A database can not have both CCR binlog and changelog, and a CCR database holds no changelog table.
    test {
        sql """
            CREATE DATABASE test_changelog_property_keys_invalid_db PROPERTIES (
                "binlog.enable" = "true",
                "changelog.enable" = "true"
            )
        """
        exception "Database test_changelog_property_keys_invalid_db can not enable both binlog and changelog"
    }
    sql """
        CREATE DATABASE test_changelog_property_keys_ccr_db PROPERTIES (
            "binlog.enable" = "true"
        )
    """
    test {
        sql """ALTER DATABASE test_changelog_property_keys_ccr_db SET PROPERTIES ("changelog.enable" = "true")"""
        exception "Database test_changelog_property_keys_ccr_db can not enable both binlog and changelog"
    }
    test {
        sql """
            CREATE TABLE test_changelog_property_keys_ccr_db.test_changelog_property_keys_ccr_db_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "changelog.enable" = "true"
            )
        """
        exception "Can not create table test_changelog_property_keys_ccr_db_tbl with changelog in database " +
                "test_changelog_property_keys_ccr_db because the database enables binlog"
    }

    // A dropped changelog table can not be recovered while its database enables binlog, and it keeps its
    // changelog once it is recovered.
    sql "CREATE DATABASE test_changelog_property_keys_recover_db"
    sql """
        CREATE TABLE test_changelog_property_keys_recover_db.test_changelog_property_keys_recover_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true"
        )
    """
    sql "INSERT INTO test_changelog_property_keys_recover_db.test_changelog_property_keys_recover_tbl VALUES (1, 10), (2, 20)"
    sql "DROP TABLE test_changelog_property_keys_recover_db.test_changelog_property_keys_recover_tbl"
    sql """ALTER DATABASE test_changelog_property_keys_recover_db SET PROPERTIES ("binlog.enable" = "true")"""
    test {
        sql "RECOVER TABLE test_changelog_property_keys_recover_db.test_changelog_property_keys_recover_tbl"
        exception "Can not recover table test_changelog_property_keys_recover_tbl with changelog in database " +
                "test_changelog_property_keys_recover_db because the database enables binlog"
    }
    sql """ALTER DATABASE test_changelog_property_keys_recover_db SET PROPERTIES ("binlog.enable" = "false")"""
    sql "RECOVER TABLE test_changelog_property_keys_recover_db.test_changelog_property_keys_recover_tbl"
    order_qt_recovered_table_format """
        SELECT PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.table_properties
        WHERE TABLE_CATALOG = 'internal'
            AND TABLE_SCHEMA = 'test_changelog_property_keys_recover_db'
            AND TABLE_NAME = 'test_changelog_property_keys_recover_tbl'
            AND PROPERTY_NAME IN ('binlog.enable', 'changelog.enable')
    """
    order_qt_recovered_changelog """
        SELECT __DORIS_CHANGELOG_OP__ AS op, k1, v1
        FROM changelog("db" = "test_changelog_property_keys_recover_db",
            "table" = "test_changelog_property_keys_recover_tbl")
    """
}
