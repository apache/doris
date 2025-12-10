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

suite("test_non_overlap_seg_heavy_sc", "nonConcurrent") {
    def tblName = "test_non_overlap_seg_heavy_sc"
    sql """
        DROP TABLE IF EXISTS ${tblName}
        """

    sql """
        CREATE TABLE IF NOT EXISTS ${tblName}
        (
            k INT NOT NULL,
            v1 INT NOT NULL,
            v2 INT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "light_schema_change" = "true"
        ); 
        """

    GetDebugPoint().clearDebugPointsForAllBEs();
    GetDebugPoint().clearDebugPointsForAllBEs();
    GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush");
    try {
        sql """ INSERT INTO ${tblName} select number, number, number from numbers("number" = "1240960") """

        sql """ DELETE FROM ${tblName} WHERE v2 = 24 """

        sql """ ALTER TABLE ${tblName} DROP COLUMN v2"""

        sql """ ALTER TABLE ${tblName} MODIFY COLUMN v1 STRING NOT NULL """

        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tblName}' ORDER BY createtime DESC LIMIT 1 """
            time 600
        }

        qt_sql """ SELECT count(*) FROM ${tblName} """
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs();
    }
}
