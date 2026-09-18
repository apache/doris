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

import org.apache.doris.regression.suite.ClusterOptions

suite('test_inverted_index_sc_version1_race', 'nonConcurrent,docker') {

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableDebugPoints()
    options.beConfigs += ['enable_java_support=false']
    options.beConfigs += ['alter_tablet_worker_count=1']
    options.beNum = 1

    docker(options) {
        def tableName = 'test_inverted_index_sc_version1_race'
        def scBlock = 'CloudSchemaChangeJob::process_alter_tablet.block'

        def getJobState = { tbl ->
            def result = sql """SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY CreateTime DESC LIMIT 1"""
            logger.info("getJobState: ${result}")
            if (result == null || result.isEmpty()) {
                return ''
            }
            return result[0][9]
        }

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT NOT NULL,
                title STRING NOT NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        GetDebugPoint().enableDebugPointForAllBEs(scBlock)

        try {
            sql """ALTER TABLE ${tableName} ADD INDEX idx_title (title) USING INVERTED PROPERTIES(\"parser\" = \"english\")"""

            int runningTries = 60
            while (runningTries-- > 0) {
                if (getJobState(tableName) == 'RUNNING') {
                    break
                }
                sleep(1000)
            }
            assertEquals('RUNNING', getJobState(tableName))

            sql """INSERT INTO ${tableName} VALUES (1, 'alpha beta'), (2, 'beta gamma'), (3, 'gamma delta')"""
            assertEquals(3L, (sql "SELECT count(*) FROM ${tableName}")[0][0])

        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(scBlock)
        }

        int maxTries = 180
        def finalState = ''
        while (maxTries-- > 0) {
            finalState = getJobState(tableName)
            if (finalState == 'FINISHED' || finalState == 'CANCELLED') {
                break
            }
            sleep(1000)
        }
        assertEquals('FINISHED', finalState)

        def showIndexResult = sql "SHOW INDEX FROM ${tableName}"
        assertTrue(showIndexResult.any { row -> row[2].toString() == 'idx_title' })

        def queryResult = sql "SELECT id FROM ${tableName} WHERE title MATCH 'beta' ORDER BY id"
        assertEquals([[1], [2]], queryResult)
    }
}
