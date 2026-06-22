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

suite("test_ivm_complete_refresh_rowid", "mtmv") {
    String tableName = "ivm_rowid_base"
    String mvName = "ivm_rowid_mv"

    sql """SET show_hidden_columns = false;"""
    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists ${tableName};"""

    sql """
        CREATE TABLE ${tableName} (
            k1 INT,
            v1 INT,
            v2 VARCHAR(50)
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """

    sql """
        INSERT INTO ${tableName} VALUES
            (1, 10, 'aaa'),
            (2, 20, 'bbb'),
            (3, 30, 'ccc');
    """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT * FROM ${tableName};
    """

    def queryMvRowsWithHiddenRowId = {
        sql """SET show_hidden_columns = true;"""
        def result = sql """
            SELECT __DORIS_IVM_ROW_ID_COL__, k1, v1, v2
            FROM ${mvName}
            ORDER BY k1, v1, v2, __DORIS_IVM_ROW_ID_COL__;
        """
        sql """SET show_hidden_columns = false;"""
        return result
    }

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;"""
    waitingMTMVTaskFinishedByMvName(mvName)
    def firstRefreshResult = queryMvRowsWithHiddenRowId()

    sql """REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;"""
    waitingMTMVTaskFinishedByMvName(mvName)
    def secondRefreshResult = queryMvRowsWithHiddenRowId()

    logger.info("firstRefreshResult=${firstRefreshResult}")
    logger.info("secondRefreshResult=${secondRefreshResult}")

    assertEquals(3, firstRefreshResult.size())
    assertEquals(firstRefreshResult.size(), secondRefreshResult.size())

    for (int i = 0; i < firstRefreshResult.size(); i++) {
        def firstRow = firstRefreshResult[i]
        def secondRow = secondRefreshResult[i]
        assertEquals(firstRow[1], secondRow[1])
        assertEquals(firstRow[2], secondRow[2])
        assertEquals(firstRow[3], secondRow[3])
        assertTrue(firstRow[0].toString() != secondRow[0].toString(),
                "row id should change after complete refresh for row: ${firstRow}")
    }

}
