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


// A table stream is listed as a BASE TABLE whose engine is Stream; a view stays a VIEW.
suite("test_stream_table_type") {
    sql "DROP DATABASE IF EXISTS test_stream_table_type_db FORCE"
    sql "CREATE DATABASE test_stream_table_type_db"
    sql "USE test_stream_table_type_db"

    sql """
        CREATE TABLE `stream_base` (
          `sid` int NULL,
          `sname` varchar(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`sid`)
        DISTRIBUTED BY HASH(`sid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "changelog.enable" = "true"
        );
    """
    sql """
        CREATE STREAM `user_stream` ON TABLE stream_base
        COMMENT 'user stream'
        PROPERTIES('type' = 'append_only');
    """
    sql "CREATE VIEW `base_view` AS SELECT `sid`, `sname` FROM stream_base"

    qt_show_full_tables_stream "SHOW FULL TABLES FROM test_stream_table_type_db LIKE 'user_stream'"
    qt_show_full_tables_view "SHOW FULL TABLES FROM test_stream_table_type_db LIKE 'base_view'"
    order_qt_information_schema_tables """
        SELECT TABLE_NAME, TABLE_TYPE, ENGINE
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = 'test_stream_table_type_db'
    """
    // Only the view is a view
    order_qt_information_schema_views """
        SELECT TABLE_NAME FROM information_schema.views WHERE TABLE_SCHEMA = 'test_stream_table_type_db'
    """
}
