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
import org.apache.doris.regression.suite.ClusterOptions

suite("test_schema_change_with_group_commit", "docker") {
    def options = new ClusterOptions()
    options.feConfigs += [
            'wait_internal_group_commit_finish=true',
            'group_commit_interval_ms_default_value=2'
    ]
    options.beConfigs += [
            'group_commit_replay_wal_retry_num=2',
            'group_commit_replay_wal_retry_interval_seconds=1',
            'group_commit_wait_replay_wal_finish=true',
            'wait_internal_group_commit_finish=true'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    docker(options) {
        def tableName3 = "test_schema_change_with_group_commit"

        def getJobState = { tableName ->
            def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            return jobStateResult[0][9]
        }
        def execStreamLoad = {
            streamLoad {
                table "${tableName3}"

                set 'column_separator', ','

                file 'all_types.csv'
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    if (json.Status.toLowerCase() == "success") {
                        assertEquals(2500, json.NumberTotalRows)
                        assertEquals(0, json.NumberFilteredRows)
                    } else {
                        assertTrue(json.Message.contains("blocked on schema change"))
                    }
                }
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName3} """

        sql """
    CREATE TABLE IF NOT EXISTS ${tableName3} (
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
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
    );
    """

        execStreamLoad()

        sql """ alter table ${tableName3} modify column k4 string NULL"""

        Awaitility.await().atMost(30, TimeUnit.SECONDS).pollDelay(10, TimeUnit.MILLISECONDS).pollInterval(10, TimeUnit.MILLISECONDS).until(
                {
                    String res = getJobState(tableName3)
                    if (res == "FINISHED" || res == "CANCELLED") {
                        assertEquals("FINISHED", res)
                        return true
                    }
                    execStreamLoad()
                    return false
                }
        )
    }

}