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

suite("test_index_change_format", "inverted_index_format_v2"){
    def createAndAlterTable = { tableName, initialFormat, newFormat -> 
        sql "DROP TABLE IF EXISTS ${tableName}"
        
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `score` int(11) NULL,
                index index_name (name) using inverted,
                index index_score (score) using inverted
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "inverted_index_storage_format" = "${initialFormat}",
                "disable_auto_compaction" = "true"
            );
        """
        try {
            sql "ALTER TABLE ${tableName} SET ('inverted_index_storage_format' = '${newFormat}')"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains('Property inverted_index_storage_format is not allowed to change'))
        }
    }

    createAndAlterTable("test_index_change_format_v1", "V1", "V2")
    createAndAlterTable("test_index_change_format_v2", "V2", "V1")
}
