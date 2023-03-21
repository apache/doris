
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

suite("test_default_current_timestamp") {
    def tbName = "test_default_current_timestamp"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                `house_id` bigint(20) NULL DEFAULT "-1" COMMENT '仓库id',
                `pick_order_big_num` decimal(27, 9) NULL
            )
            UNIQUE KEY(house_id)
            DISTRIBUTED BY HASH(house_id) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName} values(1,1.1)"
    sql "insert into ${tbName} values(2,1.1)"

    sql """ ALTER TABLE ${tbName} ADD COLUMN compute_time datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '计算时间' AFTER pick_order_big_num; """

    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }
    int max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    sql """sync"""
    qt_sql """ SELECT COUNT(*) FROM ${tbName} WHERE date(compute_time) = curdate() """
    sql "DROP TABLE ${tbName}"
}
