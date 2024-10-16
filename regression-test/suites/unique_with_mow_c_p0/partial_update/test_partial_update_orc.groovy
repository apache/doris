
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

suite("test_primary_key_partial_update_orc", "p0") {
    def tableName = "test_primary_key_partial_update_orc"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `col_0` int(11) NOT NULL COMMENT "col_0",
                `col_1` varchar(65533) NOT NULL COMMENT "col_1",
                `col_2` varchar(65533) NOT NULL COMMENT "col_2",
                `col_3` varchar(65533) NULL COMMENT "col_3",
                `col_4` varchar(65533) DEFAULT "4321")
                UNIQUE KEY(`col_0`)
                CLUSTER BY(`col_1`, `col_2`) 
                DISTRIBUTED BY HASH(`col_0`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """

    streamLoad {
        table "${tableName}"

        set 'format', 'orc'

        file 'basic.orc'
        time 10000 // limit inflight 10s
    }

    streamLoad {
        table "${tableName}"

        set 'format', 'orc'
        set 'partial_columns', 'true'
        set 'columns', 'col_0,col_1'

        file 'update.orc'
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

    qt_select_0 """
        select * from ${tableName} order by col_0;
    """

    // drop drop
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
