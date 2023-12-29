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

suite ("reorder_mor") {

    sql """ DROP TABLE IF EXISTS test; """

    sql """
            create table test (c0 int, c1 int, c2 int) ENGINE=OLAP UNIQUE KEY(`c0`, c1) COMMENT 'OLAP' DISTRIBUTED BY HASH(`c0`) BUCKETS 1 PROPERTIES ("replication_allocation" = "tag.location.default: 1", "enable_unique_key_merge_on_write"="false");
        """

    sql "insert into test values(1,8,3),(2,9,3),(3,1,3),(4,2,3);"

    sql "alter table test order by(c1,c0,c2);"

    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    int max_try_time = 100
    while (max_try_time--){
        String result = getJobState("test")
        if (result == "FINISHED") {
            sleep(1000)
            break
        } else {
            sleep(1000)
            assertTrue(max_try_time>1)
        }
    }

    sql "insert into test values(1, 3, 5);"

    qt_select_star "select * from test order by 1,2,3;"
}
