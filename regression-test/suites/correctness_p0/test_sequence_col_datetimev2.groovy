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

suite("test_sequence_col_datetimev2") {

    def tableName0 = "test_sequence_col_datetimev2"

    sql """ DROP TABLE IF EXISTS ${tableName0} """

    sql """
        CREATE TABLE ${tableName0} 
        (
            project_id BIGINT NOT NULL ,
            consume_date DATETIMEV2 NOT NULL,
            order_id BIGINT NOT NULL ,
            combo_extend JSONB DEFAULT NULL,
            age INT,
            name varchar(20),
            write_time DATETIME DEFAULT CURRENT_TIMESTAMP
        ) UNIQUE KEY(project_id, consume_date,order_id)
        DISTRIBUTED BY HASH(project_id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "function_column.sequence_type" = "datetimev2"
        );
    """

    def res = sql " show create table ${tableName0} "

    logger.info("show create table ${tableName0} : " + res[0][1])

    assertTrue(res[0][1].contains("\"function_column.sequence_type\" = \"datetimev2(0)\""))

    sql """ DROP TABLE IF EXISTS ${tableName0} """

    sql (res[0][1])

    def show_res = sql " show create table ${tableName0} "

    logger.info("show create table ${tableName0} : " + show_res[0][1])

    assertTrue(res[0][1].contains("\"function_column.sequence_type\" = \"datetimev2(0)\""))

    def tableName1 = "test_sequence_col_datetimev2_1"

    sql """ DROP TABLE IF EXISTS ${tableName1} """

    sql """
        CREATE TABLE ${tableName1} 
        (
            project_id BIGINT NOT NULL ,
            consume_date DATETIMEV2 NOT NULL,
            order_id BIGINT NOT NULL ,
            combo_extend JSONB DEFAULT NULL,
            age INT,
            name varchar(20),
            write_time DATETIME DEFAULT CURRENT_TIMESTAMP
        ) UNIQUE KEY(project_id, consume_date,order_id)
        DISTRIBUTED BY HASH(project_id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "function_column.sequence_type" = "datetimev2(3)"
        );
    """

    res = sql " show create table ${tableName1} "

    logger.info("show create table ${tableName1} : " + res[0][1])

    assertTrue(res[0][1].contains("\"function_column.sequence_type\" = \"datetimev2(3)\""))

    sql """ DROP TABLE IF EXISTS ${tableName1} """

    sql (res[0][1])

    show_res = sql " show create table ${tableName1} "

    logger.info("show create table ${tableName1} : " + show_res[0][1])

    assertTrue(res[0][1].contains("\"function_column.sequence_type\" = \"datetimev2(3)\""))
}

