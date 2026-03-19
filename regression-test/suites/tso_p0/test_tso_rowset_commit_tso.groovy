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

import org.apache.doris.regression.util.Http

suite("test_tso_rowset_commit_tso", "nonConcurrent") {
    def ret = sql "SHOW FRONTEND CONFIG like '%experimental_enable_feature_tso%';"
    logger.info("${ret}")
    try {
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_feature_tso' = 'true')"
        sleep(1000)
        def url = String.format("http://%s/api/tso", context.config.feHttpAddress)
        def tsoResp = Http.GET(url, true)
        if (tsoResp.code != 0) {
            logger.info("tso api not available, skip test_tso_rowset_commit_tso")
            return
        }

        def tableName = "test_tso_rowset_commit_tso"
        sql """DROP TABLE IF EXISTS ${tableName}"""
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id INT
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "enable_tso" = "true", "disable_auto_compaction" = "true")
        """

        sql """INSERT INTO ${tableName} VALUES (1), (2), (3)"""

        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        assertTrue(tablets.size() > 0)
        def tabletId = tablets[0]["TabletId"]

        def commitTso = -1L
        for (int i = 0; i < 10; i++) {
            def rowsets = sql_return_maparray """
                select COMMIT_TSO from information_schema.rowsets
                where TABLET_ID = ${tabletId}
                order by TXN_ID desc limit 1
            """
            if (rowsets.size() > 0) {
                commitTso = ((Number) rowsets[0]["COMMIT_TSO"]).longValue()
            }
            if (commitTso > 0) {
                break
            }
            Thread.sleep(1000)
        }

        assertTrue(commitTso > 0)
        assertTrue(commitTso >= ((Number) tsoResp.data.current_tso).longValue())

        sql """DROP TABLE IF EXISTS ${tableName}"""
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('experimental_enable_feature_tso' = '${ret[0][1]}')"
    }
}

