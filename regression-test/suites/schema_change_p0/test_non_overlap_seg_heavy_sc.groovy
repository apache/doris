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

suite("test_non_overlap_seg_heavy_sc") {
    def tblName = "test_non_overlap_seg_heavy_sc"
    sql """
        DROP TABLE IF EXISTS ${tblName}_src
        """
    sql """
        CREATE TABLE IF NOT EXISTS ${tblName}_src
        (
            k INT NOT NULL,
            v1 INT NOT NULL,
            v2 INT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 5
        PROPERTIES(
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        ); 
        """

    sql """
        DROP TABLE IF EXISTS ${tblName}_dst
        """
    sql """
        CREATE TABLE IF NOT EXISTS ${tblName}_dst
        (
            k INT NOT NULL,
            v1 INT NOT NULL,
            v2 INT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        ); 
        """

    sql """ INSERT INTO ${tblName}_src VALUES (1, 1, 1),(2, 2, 2),(3, 3, 3),(4, 4, 4),(5, 5, 5) """

    sql """ INSERT INTO ${tblName}_dst SELECT * FROM ${tblName}_src """

    sql """ DELETE FROM ${tblName}_dst WHERE v1 = 1 """

    sql """ ALTER TABLE ${tblName}_dst DROP COLUMN v1"""

    sql """ ALTER TABLE ${tblName}_dst MODIFY COLUMN v2 STRING NOT NULL """

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tblName}_dst' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    qt_sql """ SELECT * FROM ${tblName}_dst ORDER BY k """
}
