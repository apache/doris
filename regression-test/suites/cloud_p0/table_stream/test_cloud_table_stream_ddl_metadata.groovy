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

suite("test_cloud_table_stream_ddl_metadata") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_ddl_metadata_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_ddl_metadata_db"
    sql "USE test_cloud_table_stream_ddl_metadata_db"

    sql """
        CREATE TABLE ddl_source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    sql """
        CREATE STREAM ddl_stream ON TABLE ddl_source
        COMMENT 'cloud ddl metadata stream'
    """

    order_qt_show_streams "SHOW STREAMS"
    qt_show_create_stream "SHOW CREATE STREAM ddl_stream"
    order_qt_information_schema_stream """
        SELECT DB_NAME,
               STREAM_NAME,
               STREAM_TYPE,
               CONSUME_TYPE,
               STREAM_COMMENT,
               BASE_TABLE_NAME,
               BASE_TABLE_DB,
               BASE_TABLE_CTL,
               BASE_TABLE_TYPE,
               ENABLED,
               IS_STALE,
               STALE_REASON
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_table_stream_ddl_metadata_db'
        ORDER BY STREAM_NAME
    """

    test {
        sql "CREATE STREAM ddl_stream ON TABLE ddl_source"
        exception "Table 'ddl_stream' already exists"
    }
    sql "CREATE STREAM IF NOT EXISTS ddl_stream ON TABLE ddl_source"
    qt_if_not_exists_keeps_one_stream """
        SELECT count(*)
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_table_stream_ddl_metadata_db'
          AND STREAM_NAME = 'ddl_stream'
    """

    test {
        sql """
            CREATE STREAM invalid_type_stream ON TABLE ddl_source
            PROPERTIES ("type" = "unsupported_type")
        """
        exception "not supported type: unsupported_type"
    }
    test {
        sql """
            CREATE STREAM invalid_property_stream ON TABLE ddl_source
            PROPERTIES ("unsupported_property" = "value")
        """
        exception "Unknown properties"
    }
    test {
        sql "CREATE STREAM missing_base_stream ON TABLE missing_base_table"
        exception "Unknown table 'missing_base_table'"
    }
    test {
        sql "DROP STREAM ddl_stream"
        exception "only supports DROP STREAM ... FORCE"
    }

    order_qt_stream_remains_after_non_force_drop "SHOW STREAMS"

    sql "DROP USER IF EXISTS 'cloud_stream_ddl_user'@'%'"
    sql "CREATE USER 'cloud_stream_ddl_user'@'%' IDENTIFIED BY 'CloudStream123!'"
    def noDbJdbcUrl = context.config.jdbcUrl.replaceFirst(/(jdbc:mysql:\/\/[^\/]+\/)[^?]*/, '$1')
    sql """
        GRANT SELECT_PRIV ON internal.test_cloud_table_stream_ddl_metadata_db.ddl_source
        TO 'cloud_stream_ddl_user'@'%'
    """
    connect('cloud_stream_ddl_user', 'CloudStream123!', noDbJdbcUrl) {
        test {
            sql """
                CREATE STREAM test_cloud_table_stream_ddl_metadata_db.denied_stream
                ON TABLE test_cloud_table_stream_ddl_metadata_db.ddl_source
            """
            exception "denied"
        }
    }

    sql """
        GRANT CREATE_PRIV ON internal.test_cloud_table_stream_ddl_metadata_db.*
        TO 'cloud_stream_ddl_user'@'%'
    """
    connect('cloud_stream_ddl_user', 'CloudStream123!', noDbJdbcUrl) {
        sql """
            CREATE STREAM test_cloud_table_stream_ddl_metadata_db.priv_stream
            ON TABLE test_cloud_table_stream_ddl_metadata_db.ddl_source
        """
        test {
            sql "DROP STREAM test_cloud_table_stream_ddl_metadata_db.priv_stream FORCE"
            exception "denied"
        }
    }

    sql """
        GRANT DROP_PRIV ON internal.test_cloud_table_stream_ddl_metadata_db.priv_stream
        TO 'cloud_stream_ddl_user'@'%'
    """
    connect('cloud_stream_ddl_user', 'CloudStream123!', noDbJdbcUrl) {
        sql "DROP STREAM test_cloud_table_stream_ddl_metadata_db.priv_stream FORCE"
    }
    sql "DROP USER 'cloud_stream_ddl_user'@'%'"
}
