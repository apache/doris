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

suite("test_index_bkd_writer_fault_injection", "nonConcurrent") {
    def isCloudMode = isCloudMode()
    def tableName = "test_index_bkd_writer_fault_injection"

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("parser"="english"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_num" = "1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "V1");
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::add_value_bkd_writer_add_throw_error")
        logger.info("trigger_full_compaction_on_tablets with fault injection: InvertedIndexColumnWriterImpl::add_value_bkd_writer_add_throw_error")
        sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    } catch (Exception e) {
        logger.info("error message: ${e.getMessage()}")
        assert e.getMessage().contains("packedValue should be length=xxx")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("InvertedIndexColumnWriterImpl::add_value_bkd_writer_add_throw_error")
    }
}
