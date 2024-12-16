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

suite("test_cloud_mow_delete_bitmap_calc_timeout","nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def tableName = "test_cloud_mow_delete_bitmap_calc_timeout"
 
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName}
            (k bigint,   v1 string, v2 string, v3 string, v4 string )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH (k) 
            BUCKETS 4
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write"="true");
        """

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    try {
        sql "insert into ${tableName} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
        qt_select_1 "select * from ${tableName} order by k;"
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.calc_delete_bitmap_random_timeout", [percent: 1, execute: 4])
        sql "insert into ${tableName} values(1,2,2,2,2),(3,4,4,4,4);"
    } catch (Exception e) {
        logger.info(e.getMessage())
        AssertTrue(false) 
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.calc_delete_bitmap_random_timeout")
        qt_select_2 "select * from ${tableName} order by k;"
    }
}
