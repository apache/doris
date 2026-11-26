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

// The "binlog.*" properties configure CCR binlog. A key under "binlog." other than binlog.enable,
// binlog.ttl_seconds, binlog.max_bytes and binlog.max_history_nums is rejected instead of being
// ignored, and binlog.enable accepts only true or false, in any case.
suite("test_binlog_property_validation") {
    sql "DROP TABLE IF EXISTS test_binlog_property_validation_tbl"

    test {
        sql """
            CREATE TABLE test_binlog_property_validation_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.ttl_second" = "3600"
            )
        """
        exception "Unknown binlog property: binlog.ttl_second"
    }
    test {
        sql """
            CREATE TABLE test_binlog_property_validation_tbl (
                k1 INT NOT NULL,
                v1 INT
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "yes"
            )
        """
        exception "Invalid binlog enable value: yes"
    }

    sql """
        CREATE TABLE test_binlog_property_validation_tbl (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "False"
        )
    """
    order_qt_table_binlog_enable """
        SELECT TABLE_NAME, PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.table_properties
        WHERE TABLE_CATALOG = 'internal'
            AND TABLE_SCHEMA = '${context.dbName}'
            AND TABLE_NAME = 'test_binlog_property_validation_tbl'
            AND PROPERTY_NAME = 'binlog.enable'
    """

    // The unknown key used to fail with "Nothing is changed", and "yes" used to be read as false,
    // which turned binlog off on a table that had it on.
    test {
        sql """ALTER TABLE test_binlog_property_validation_tbl SET ("binlog.ttl_second" = "3600")"""
        exception "Unknown binlog property: binlog.ttl_second"
    }
    test {
        sql """ALTER TABLE test_binlog_property_validation_tbl SET ("binlog.enable" = "yes")"""
        exception "Invalid binlog enable value: yes"
    }

    sql "DROP DATABASE IF EXISTS test_binlog_property_validation_db"
    test {
        sql """CREATE DATABASE test_binlog_property_validation_db PROPERTIES ("binlog.ttl_second" = "3600")"""
        exception "Unknown binlog property: binlog.ttl_second"
    }
    sql "CREATE DATABASE test_binlog_property_validation_db"
    // Database properties are stored as given, so these used to be persisted as they were.
    test {
        sql """ALTER DATABASE test_binlog_property_validation_db SET PROPERTIES ("binlog.max_byte" = "1024")"""
        exception "Unknown binlog property: binlog.max_byte"
    }
    test {
        sql """ALTER DATABASE test_binlog_property_validation_db SET PROPERTIES ("binlog.enable" = "1")"""
        exception "Invalid binlog enable value: 1"
    }
    order_qt_database_binlog_properties """
        SELECT SCHEMA_NAME, PROPERTY_NAME, PROPERTY_VALUE
        FROM information_schema.database_properties
        WHERE SCHEMA_NAME = 'test_binlog_property_validation_db'
            AND PROPERTY_NAME LIKE 'binlog.%'
    """
}
