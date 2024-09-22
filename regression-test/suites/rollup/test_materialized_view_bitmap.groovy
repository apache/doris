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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_materialized_view_bitmap", "rollup") {

    def tbName1 = "test_materialized_view_bitmap"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1}(
                k1 BOOLEAN NOT NULL,
                k2 TINYINT NOT NULL,
                k3 SMALLINT NOT NULL
            ) 
            DISTRIBUTED BY HASH(k1) properties("replication_num" = "1");
        """

    sql "CREATE MATERIALIZED VIEW test_neg as select k1,bitmap_union(to_bitmap(k2)), bitmap_union(to_bitmap(k3)) FROM ${tbName1} GROUP BY k1;"
    max_try_secs = 60
    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true;
        }
        return false;
    }

    explain {
        sql "insert into ${tbName1} values(1,1,1);"
        contains "to_bitmap_with_check"
    }
    sql "insert into ${tbName1} values(1,1,1);"
    sql "insert into ${tbName1} values(0,1,1);"

    test {
        sql "insert into ${tbName1} values(1,-1,-1);"
        // check exception message contains
        exception "The input: -1 is not valid, to_bitmap only support bigint value from 0 to 18446744073709551615 currently"
    }

    sql "DROP TABLE ${tbName1} FORCE;"
}
