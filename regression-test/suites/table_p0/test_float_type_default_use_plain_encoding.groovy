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

import org.apache.doris.regression.util.Http

suite('test_float_type_default_use_plain_encoding') {

    // Test 1: Property explicitly set to true
    def tableName1 = "test_float_plain_encoding_enabled"
    sql """drop table if exists `${tableName1}` force; """

    sql """
        CREATE TABLE ${tableName1}
        (k int, v1 float, v2 double)
        duplicate KEY(k)
        DISTRIBUTED BY HASH (k)
        BUCKETS 1  PROPERTIES(
        "replication_num" = "1",
        "float_type_default_use_plain_encoding" = "true");
        """

    sql "insert into ${tableName1} values(1, 1.5, 2.5);"
    sql "select * from ${tableName1};"

    def metaUrl = sql_return_maparray("show tablets from ${tableName1};").get(0).MetaUrl
    logger.info("begin curl ${metaUrl}")
    def jsonMeta = Http.GET(metaUrl, true, false)

    assert jsonMeta.schema.float_type_default_use_plain_encoding == true

    // Verify property is shown in SHOW CREATE TABLE
    def createTableStmt = sql "show create table ${tableName1};"
    logger.info("show create table: ${createTableStmt}")
    assert createTableStmt[0][1].contains('"float_type_default_use_plain_encoding" = "true"')

    // Test 2: Property explicitly set to false
    def tableName2 = "test_float_plain_encoding_disabled"
    sql """drop table if exists `${tableName2}` force; """

    sql """
        CREATE TABLE ${tableName2}
        (k int, v1 float, v2 double)
        duplicate KEY(k)
        DISTRIBUTED BY HASH (k)
        BUCKETS 1  PROPERTIES(
        "replication_num" = "1",
        "float_type_default_use_plain_encoding" = "false");
        """

    sql "insert into ${tableName2} values(1, 1.5, 2.5);"
    sql "select * from ${tableName2};"

    metaUrl = sql_return_maparray("show tablets from ${tableName2};").get(0).MetaUrl
    logger.info("begin curl ${metaUrl}")
    jsonMeta = Http.GET(metaUrl, true, false)

    assert jsonMeta.schema.float_type_default_use_plain_encoding == false

    // Verify SHOW CREATE TABLE does NOT show false (only shown when true)
    createTableStmt = sql "show create table ${tableName2};"
    assert !createTableStmt[0][1].contains('float_type_default_use_plain_encoding')

    // Test 3: Property is immutable - ALTER TABLE should fail
    test {
        sql """ALTER TABLE ${tableName2} SET ("float_type_default_use_plain_encoding" = "true");"""
        exception "You can not modify float_type_default_use_plain_encoding property"
    }

    // Cleanup
    sql """drop table if exists `${tableName1}` force; """
    sql """drop table if exists `${tableName2}` force; """
}
