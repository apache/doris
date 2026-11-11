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

// A CHAR(255) column and a CHAR without a length (CAST(x AS CHAR)) are both the wildcard CHAR.
// SHOW COLUMNS and DESC print them as char(255), the name information_schema.columns gives them,
// so a client that maps type names never meets "character". CREATE TABLE LIKE re-parses the
// printed column definitions, so it has to keep the same types.
suite("test_wildcard_char_type_name") {
    sql "DROP VIEW IF EXISTS v_wildcard_char_type_name"
    sql "DROP TABLE IF EXISTS t_wildcard_char_type_name_like"
    sql "DROP TABLE IF EXISTS t_wildcard_char_type_name"
    sql """
        CREATE TABLE t_wildcard_char_type_name (
            `k` INT NOT NULL,
            `c_char_10` CHAR(10),
            `c_char_255` CHAR(255),
            `c_arr_char_255` ARRAY<CHAR(255)>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE VIEW v_wildcard_char_type_name AS
        SELECT `k`, CAST(`c_char_10` AS CHAR) AS `c_cast_char`, `c_char_255`
        FROM t_wildcard_char_type_name
    """
    sql """INSERT INTO t_wildcard_char_type_name VALUES (1, 'a', 'b', ['c'])"""

    order_qt_show_full_columns_table "SHOW FULL COLUMNS FROM t_wildcard_char_type_name"
    order_qt_show_full_columns_view "SHOW FULL COLUMNS FROM v_wildcard_char_type_name"
    order_qt_show_columns_table "SHOW COLUMNS FROM t_wildcard_char_type_name"
    order_qt_desc_table "DESC t_wildcard_char_type_name"
    order_qt_desc_view "DESC v_wildcard_char_type_name"
    order_qt_information_schema """
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
        FROM information_schema.columns
        WHERE TABLE_SCHEMA = '${context.dbName}'
            AND TABLE_NAME IN ('t_wildcard_char_type_name', 'v_wildcard_char_type_name')
    """
    order_qt_select_view "SELECT * FROM v_wildcard_char_type_name"

    sql "CREATE TABLE t_wildcard_char_type_name_like LIKE t_wildcard_char_type_name"
    sql "INSERT INTO t_wildcard_char_type_name_like SELECT * FROM t_wildcard_char_type_name"
    order_qt_create_table_like_columns "SHOW FULL COLUMNS FROM t_wildcard_char_type_name_like"
    order_qt_create_table_like_select "SELECT * FROM t_wildcard_char_type_name_like"
}
