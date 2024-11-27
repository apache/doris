
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

suite("test_partial_update_with_inverted_index", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"
            def tableName = "test_partial_update_with_inverted_index"
            // create table
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                        col1 BOOLEAN,
                        col2 TINYINT,
                        col3 SMALLINT,
                        col4 variant,
                        INDEX idx_col1 (`col1`) USING INVERTED,
                        INDEX idx_col2 (`col2`) USING INVERTED,
                        INDEX idx_col3 (`col3`) USING INVERTED,
                        INDEX idx_col4 (`col4`) USING INVERTED
                    ) unique key(col1, col2) distributed by hash(col1) buckets 1
                    properties(
                        "replication_num" = "1",
                        "store_row_column" = "${use_row_store}"
                    ); """

            sql """
                insert into ${tableName} values(true, 1, 1, '{"a":"a"}');
            """
            qt_select_1 "select * from ${tableName};"

            sql "set enable_unique_key_partial_update=true;" 
            sql "set enable_insert_strict=false;"
            sql """
                insert into ${tableName} (col1, col2, col4)values(true, 1, '{"b":"b"}');
            """
            qt_select_2 "select * from ${tableName};"
        }
    }
}
