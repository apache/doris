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

suite("test_alter_table_column_rename") {
    def tbName = "alter_table_column_rename"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    sql "DROP TABLE IF EXISTS ${tbName}"
    // char not null to null
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k1 INT NOT NULL,
                value1 varchar(16) NOT NULL,
                value2 int NOT NULL
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
        """

    sql """ insert into ${tbName} values (1, 'a', 2) """
    sql """ select * from ${tbName} """

    // not nullable to nullable
    sql """ ALTER TABLE ${tbName} RENAME COLUMN value2 new_col """

    /*
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    */

    sql """ select * from ${tbName} where new_col = 2 """

}
