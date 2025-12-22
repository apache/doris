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

suite('test_storage_format_controls_encoding') {
    def tableName = "test_storage_format_controls_encoding"
    sql """drop table if exists `${tableName}` force; """

    sql """
        CREATE TABLE ${tableName}
        (k int, v1 int, v2 varchar(100))
        duplicate KEY(k)
        DISTRIBUTED BY HASH (k) 
        BUCKETS 1  PROPERTIES(
        "replication_num" = "1",
        "storage_format" = "V3");
        """
    
    sql "insert into ${tableName} values(1, 1, 'aaa');"
    sql "select * from ${tableName};"

    def metaUrl = sql_return_maparray("show tablets from ${tableName};").get(0).MetaUrl
    logger.info("begin curl ${metaUrl}")
    def jsonMeta = Http.GET(metaUrl, true, false)

    assert jsonMeta.schema.integer_type_default_use_plain_encoding == true
    assert jsonMeta.schema.binary_plain_encoding_default_impl == "BINARY_PLAIN_ENCODING_V2"


    def res = sql """show variables like "%use_v3_storage_format%";""";
    logger.info("session var use_v3_storage_format: ${res}")
    if (res[0][1] == "true") return

    tableName = "test_storage_format_controls_encoding2"
    sql """drop table if exists `${tableName}` force; """

    sql """
        CREATE TABLE ${tableName}
        (k int, v1 int, v2 varchar(100))
        duplicate KEY(k)
        DISTRIBUTED BY HASH (k) 
        BUCKETS 1  PROPERTIES(
        "replication_num" = "1",
        "storage_format" = "V2");
        """
    
    sql "insert into ${tableName} values(1, 1, 'aaa');"
    sql "select * from ${tableName};"

    metaUrl = sql_return_maparray("show tablets from ${tableName};").get(0).MetaUrl
    logger.info("begin curl ${metaUrl}")
    jsonMeta = Http.GET(metaUrl, true, false)

    assert jsonMeta.schema.integer_type_default_use_plain_encoding == false
    assert jsonMeta.schema.binary_plain_encoding_default_impl == "BINARY_PLAIN_ENCODING_V1"
}