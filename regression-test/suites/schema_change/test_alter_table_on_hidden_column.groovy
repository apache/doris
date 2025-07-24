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


suite("test_alter_table_on_hidden_column", "schema_change") {
    def tableName = "alter_table_hidden_column"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k1 INT,
                value1 INT,
                value2 INT,
                value3 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 
            properties(
                "replication_num" = "1"
            );
        """

    // rename hidden column 
    try {
        sql "ALTER TABLE ${tableName} RENAME COLUMN __DORIS_VERSION_COL__ test;"
    } catch (Exception ex) {
        assert("${ex}".contains("Do not support rename hidden column"))
    }

    // drop hidden column
    try {
        sql "ALTER TABLE ${tableName} DROP COLUMN __DORIS_VERSION_COL__;"
    } catch (Exception ex) {
        assert("${ex}".contains("Do not support drop hidden column"))
    }

    // add a column name starting with __DORIS_ 
    try {
        sql "ALTER TABLE ${tableName} ADD COLUMN __DORIS_VERSION_COL__ bigint;"
    } catch (Exception ex) {
        assert("${ex}".contains("column name can't start with"))
    }

    // add columns, a column name starting with __DORIS_ 
    try {
        sql "ALTER TABLE ${tableName} ADD COLUMN (c1 int, __DORIS_VERSION_COL__ bigint);"
    } catch (Exception ex) {
        assert("${ex}".contains("column name can't start with"))
    }

    // modify hidden column comment
    try {
        sql "ALTER TABLE ${tableName} MODIFY COLUMN __DORIS_VERSION_COL__ COMMENT 'test';"
    } catch (Exception ex) {
        assert("${ex}".contains("Unknown column"))
    }

    // modify hidden column type
    try {
        sql "ALTER TABLE ${tableName} MODIFY COLUMN __DORIS_VERSION_COL__ VARCHAR(64);"
    } catch (Exception ex) {
        assert("${ex}".contains("column name can't start with"))
    }

}
