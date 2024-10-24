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

suite ("test_uniq_mv_useless") {
    def testTable = "test_uniq_mv_useless_table"
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${testTable}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }
    sql """ DROP TABLE IF EXISTS ${testTable}; """

    sql """
            create table ${testTable} (
                k1 int null,
                k2 int null,
                k3 int 
            )
            unique key (k1,k2)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "false");
        """
        // only mor table can have mv

    sql "insert into ${testTable} select 1,1,1;"
    sql "insert into ${testTable} select 2,2,2;"
    sql "insert into ${testTable} select 3,3,3;"

    test {
        sql "create materialized view k1_k2_u12 as select k1,k2 from ${testTable} group by k1,k2;"
        exception "errCode = 2,"
    }

    createMV ("create materialized view k1_k2_u21 as select k2,k1 from ${testTable};")
    sql "insert into ${testTable} select 4,4,4;"
}
