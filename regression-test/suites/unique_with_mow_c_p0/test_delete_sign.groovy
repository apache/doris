
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

suite("test_delete_sign", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql "use ${db};"
            def tableName = "test_delete_sign"
            // test delete sigin X sequence column
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                        col1 BOOLEAN,
                        col2 INT,
                        col3 INT,
                        col4 variant
                    ) unique key(col1, col2)
                    CLUSTER BY (`col3`, `col2`) 
                    distributed by hash(col1) buckets 1
                    properties(
                        "replication_num" = "1",
                        "function_column.sequence_col" = 'col3',
                        "store_row_column" = "${use_row_store}"
                    ); """

            sql """insert into ${tableName} values(true, 1, 1, '{"a":"a"}');"""
            sql """insert into ${tableName} values(true, 1, 10, '{"c":"c"}');"""
            sql """insert into ${tableName} values(true, 1, 2, '{"b":"b"}');"""
            qt_select_0 "select * from ${tableName};"
            sql """insert into ${tableName} (col1,col2,col3,col4,__DORIS_DELETE_SIGN__)values(true, 1, 10, '{"c":"c"}',1);"""
            qt_select_1 "select * from ${tableName};"
            sql """insert into ${tableName} values(true, 1, 3, '{"c":"c"}');"""
            qt_select_2 "select * from ${tableName};"

            // test delete sigin X update
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                        col1 BOOLEAN,
                        col2 INT,
                        col3 INT,
                        col4 variant
                    ) unique key(col1, col2) 
                    CLUSTER BY (`col2`, `col3`) 
                    distributed by hash(col1) buckets 1
                    properties(
                        "replication_num" = "1",
                        "function_column.sequence_col" = 'col3',
                        "store_row_column" = "${use_row_store}"
                    ); """

            sql """insert into ${tableName} values(true, 1, 1, '{"a":"a"}');"""
            sql """insert into ${tableName} values(true, 1, 10, '{"c":"c"}');"""
            sql """insert into ${tableName} values(true, 1, 2, '{"b":"b"}');"""
            qt_select_3 "select * from ${tableName};"
            sql """insert into ${tableName} (col1,col2,col3,col4,__DORIS_DELETE_SIGN__)values(true, 1, 10, '{"c":"c"}',1);"""
            qt_select_4 "select * from ${tableName};"
            sql """insert into ${tableName} values(true, 1, 3, '{"c":"c"}');"""
            qt_select_5 "select * from ${tableName};"
            //sql """update ${tableName} set __DORIS_DELETE_SIGN__=0 where col3=10;"""
            qt_select_6 "select * from ${tableName};"
            sql """insert into ${tableName} values(true, 1, 5, '{"c":"c"}');"""

            // test delete sigin X default value
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                        col1 BOOLEAN,
                        col2 INT,
                        col3 INT,
                        col4 variant NULL
                    ) unique key(col1, col2)
                    CLUSTER BY (`col1`, `col3`)  
                    distributed by hash(col1) buckets 1
                    properties(
                        "replication_num" = "1",
                        "function_column.sequence_col" = 'col3',
                        "store_row_column" = "${use_row_store}"
                    ); """

            sql """insert into ${tableName} values(true, 1, 1, '{"a":"a"}');"""
            sql """insert into ${tableName} values(true, 1, 10, '{"c":"c"}');"""
            sql """insert into ${tableName} values(true, 1, 2, '{"b":"b"}');"""
            qt_select_7 "select * from ${tableName};"
            sql """insert into ${tableName} (col1,col2,col3)values(true, 1, 1);"""
            qt_select_8 "select * from ${tableName};"
            sql """insert into ${tableName} values(true, 1, 30, '{"b":"b"}');"""
            qt_select_9 "select * from ${tableName};"

            // test delete sigin X txn
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                        col1 BOOLEAN,
                        col2 INT,
                        col3 INT,
                        col4 variant default NULL,
                        INDEX idx_col3 (`col3`) USING INVERTED,
                    ) unique key(col1, col2)
                    CLUSTER BY (`col3`, `col1`, `col2`)  
                    distributed by hash(col1) buckets 1
                    properties(
                        "replication_num" = "1",
                        "function_column.sequence_col" = 'col3',
                        "store_row_column" = "${use_row_store}"
                    ); """

            sql """begin"""
            sql """insert into ${tableName} values(true, 1, 1, '{"a":"a"}');"""
            sql """insert into ${tableName} values(true, 1, 10, '{"c":"c"}');"""
            sql """insert into ${tableName} values(true, 1, 2, '{"b":"b"}');"""
            sql """commit"""
            qt_select_10 "select * from ${tableName};"
            sql """begin"""
            sql """insert into ${tableName} (col1,col2,col3)values(true, 1, 1);"""
            sql """commit"""
            qt_select_11 "select * from ${tableName};"
            sql """begin"""
            sql """insert into ${tableName} values(true, 1, 30, '{"b":"b"}');"""
            sql """commit"""
            qt_select_12 "select * from ${tableName};"
        }
    }
}
