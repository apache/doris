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

import org.codehaus.groovy.runtime.IOGroovyMethods
import org.apache.doris.regression.util.Http

suite("test_unique_key_mow_rowsets_deleted", "nonConcurrent"){

    def tableName = "test_unique_key_mow_rowsets_deleted1"

    // 1. requested rowsets have been deleted during partial update
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `id` int(11) NOT NULL COMMENT "用户 ID",
        `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
        `score` int(11) NOT NULL COMMENT "用户得分",
        `test` int(11) NULL COMMENT "null test",
        `dft` int(11) DEFAULT "4321")
        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true"); """

    sql """insert into ${tableName} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
    qt_1 """ select * from ${tableName} order by id; """
    try {
        GetDebugPoint().enableDebugPointForAllBEs("_append_block_with_partial_content.clear_specified_rowsets")
        sql "set enable_unique_key_partial_update=true;"
        sql "set enable_insert_strict = false;"
        sql "sync;"
        sql """insert into ${tableName}(id,score) values(2,400),(1,200),(4,400)"""
        qt_2 """ select * from ${tableName} order by id; """
        sql "set enable_unique_key_partial_update=false;"
        sql "set enable_insert_strict = true;"
        sql "sync;"
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("_append_block_with_partial_content.clear_specified_rowsets")
    }
    sql "DROP TABLE IF EXISTS ${tableName};"


    // 2. requested rowsets have been deleted during row update
    tableName = "test_unique_key_mow_rowsets_deleted2"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `id` int(11) NOT NULL COMMENT "用户 ID",
        `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
        `score` int(11) NOT NULL COMMENT "用户得分",
        `test` int(11) NULL COMMENT "null test",
        `dft` int(11) DEFAULT "4321")
        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true"); """
    sql """insert into ${tableName} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
    qt_3 """ select * from ${tableName} order by id; """
    try {
        GetDebugPoint().enableDebugPointForAllBEs("BetaRowsetWriter::_generate_delete_bitmap.clear_specified_rowsets")
        sql """insert into ${tableName} values(2, "doris666", 9999, 888, 7),(1, "doris333", 6666, 555, 4), (3, "doris222", 1111, 987, 567);"""
        qt_4 """ select * from ${tableName} order by id; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("BetaRowsetWriter::_generate_delete_bitmap.clear_specified_rowsets")
    }
    sql "DROP TABLE IF EXISTS ${tableName};"
}
