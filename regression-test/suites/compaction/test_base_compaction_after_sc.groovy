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
import org.awaitility.Awaitility

suite("test_base_compaction_after_sc") {
    def tableName = "test_base_compaction_after_sc"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int,
                `c3` int
                ) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction" = "true",
                "replication_num" = "1");
    """
    sql """ ALTER TABLE ${tableName} MODIFY COLUMN c1 VARCHAR(44) """

    def wait_for_schema_change = {
                def try_times=1000
                while(true){
                    def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
                    Thread.sleep(10)
                    if(res[0][9].toString() == "FINISHED"){
                        break;
                    }
                    assert(try_times>0)
                    try_times--
                }
            }
    wait_for_schema_change()

    def insert_data = {
        for (i in 0..100) {
            sql """ INSERT INTO ${tableName} VALUES(1, "2", 3, 4) """
            sql """ DELETE FROM ${tableName} WHERE k1=1 """
        }
    }

    insert_data()

    trigger_and_wait_compaction(tableName, "cumulative")

    insert_data()

    trigger_and_wait_compaction(tableName, "cumulative")

    insert_data()

    trigger_and_wait_compaction(tableName, "cumulative")

    trigger_and_wait_compaction(tableName, "base")
}
