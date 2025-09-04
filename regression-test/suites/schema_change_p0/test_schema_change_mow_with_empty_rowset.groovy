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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_schema_change_mow_with_empty_rowset", "p0") {
    def tableName = "test_sc_mow_with_empty_rowset"

    def getJobState = { tbl ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` int(30) NULL,
      `k5` largeint(40) NULL,
      `k6` float NULL,
      `k7` double NULL,
      `k8` decimal(9, 0) NULL,
      `k9` char(10) NULL,
      `k10` varchar(1024) NULL,
      `k11` text NULL,
      `k12` date NULL,
      `k13` datetime NULL
    ) ENGINE=OLAP
    unique KEY(k1, k2, k3)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 2
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
    );
    """

    for (int i = 0; i < 100; i++) {
        sql """ insert into ${tableName} values ($i, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """
    }   

    sql """ alter table ${tableName} modify column k4 string NULL"""

    for (int i = 0; i < 20; i++) {
        sql """ insert into ${tableName} values (100, 2, 3, 4, 5, 6.6, 1.7, 8.8,
    'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """
    }   

    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(10, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(
        {
            String res = getJobState(tableName)
            if (res == "FINISHED" || res == "CANCELLED") {
                assertEquals("FINISHED", res)
                return true
            }
            return false
        }
    )

    qt_sql """ select * from ${tableName} order by k1, k2, k3 """
}

