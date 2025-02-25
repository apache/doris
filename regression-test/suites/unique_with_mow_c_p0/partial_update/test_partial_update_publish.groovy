
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

suite("test_primary_key_partial_update_publish", "p0") {
    def tableName = "test_primary_key_partial_update_publish"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分")
                UNIQUE KEY(`id`)
                CLUSTER BY(`name`) 
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'

        file '10000.csv'
        time 10000 // limit inflight 10s
    } 
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,name'

        file '10000_update_1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            txnId = json.TxnId
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Can't do partial update on merge-on-write Unique table with cluster keys"))
        }
    }
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file '10000_update_1.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            txnId = json.TxnId
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Can't do partial update on merge-on-write Unique table with cluster keys"))
        }
    }

    sql "sync"

    qt_select_default """
        select * from ${tableName} order by id;
    """

    // drop drop
    // sql """ DROP TABLE IF EXISTS ${tableName} """
}
