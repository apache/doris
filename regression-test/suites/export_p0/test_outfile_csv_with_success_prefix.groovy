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

import org.apache.doris.regression.util.ExportTestHelper

suite("test_outfile_csv_with_success_prefix") {
    def dbName = "test_outfile_csv_with_success_prefix"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"
    def tableName = "outfile_csv_with_success_prefix"
    def tableName2 = "outfile_csv_with_success_prefix2"
    def helper = new ExportTestHelper(delegate)
    def prefix = "regression/test_outfile_csv_with_success_prefix/${UUID.randomUUID()}/"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` INT NOT NULL COMMENT "用户id"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        sql """ INSERT INTO ${tableName} VALUES (1),(2);
            """
        order_qt_select_default1 """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        // check outfile
        sql """
            SELECT * FROM ${tableName} t ORDER BY user_id INTO OUTFILE "${helper.uri(prefix)}" FORMAT AS CSV_WITH_NAMES
            PROPERTIES (${helper.storageProperties()}, "column_separator" = ",", "success_file_name"="_SUCCESS");
        """

        assertTrue(helper.keys(prefix).contains("${prefix}_SUCCESS".toString()))
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql("DROP TABLE IF EXISTS ${tableName2}")
        helper.deleteFiles(prefix)
    }
}

