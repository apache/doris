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
import org.apache.doris.regression.util.NodeType

suite("test_create_table_and_create_mv_race", "p0, docker") {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(1)
    options.feConfigs.add("sys_log_verbose_modules=org")
    options.setBeNum(1)
    options.cloudMode = true

    docker(options) {
        sql """set enable_sql_cache=false"""

        def tbl = "test_create_table_and_create_mv_race_tbl"
        def mvName = "test_create_table_and_create_mv_race_mv"

        sql "DROP TABLE IF EXISTS ${tbl}"

        cluster.injectDebugPoints(NodeType.FE, [
                "FE.createOlapTable.beforeFirstTimeDynamicPartition": [sleepMs: "10000"]
        ])

        try {
            def createDoneAt = new java.util.concurrent.atomic.AtomicLong(0L)
            def mvDoneAt = new java.util.concurrent.atomic.AtomicLong(0L)

            def createFuture = thread("create-table") {
                sql """
                    CREATE TABLE ${tbl} (
                        order_id    BIGINT,
                        create_dt   datetime,
                        username    VARCHAR(20)
                    )
                    DUPLICATE KEY(order_id)
                    PARTITION BY RANGE(create_dt) ()
                    DISTRIBUTED BY HASH(order_id) BUCKETS 1
                    PROPERTIES (
                        "replication_num" = "1",
                        "dynamic_partition.enable" = "true",
                        "dynamic_partition.time_unit" = "DAY",
                        "dynamic_partition.start" = "-2",
                        "dynamic_partition.end" = "2",
                        "dynamic_partition.prefix" = "p",
                        "dynamic_partition.create_history_partition" = "true"
                    )
                """
                createDoneAt.set(System.currentTimeMillis())
            }

            sleep(2000)

            def mvFuture = thread("create-mv") {
                def attempts = 30
                while (attempts-- > 0) {
                    try {
                        sql """
                            CREATE MATERIALIZED VIEW ${mvName} AS
                            SELECT username, count(order_id)
                            FROM ${tbl}
                            GROUP BY username
                        """
                        break
                    } catch (Exception e) {
                        def msg = e.getMessage() ?: ""
                        if (msg.contains("Unknown table") || msg.contains("does not exist")) {
                            sleep(200)
                            continue
                        }
                        throw e
                    }
                }
                mvDoneAt.set(System.currentTimeMillis())
            }

            createFuture.get()
            mvFuture.get()

            def ct = createDoneAt.get()
            def mt = mvDoneAt.get()
            assert ct > 0 && mt > 0 : "both futures should have completed"
            assert mt >= ct : "CREATE MV (${mt}) must finish after CREATE TABLE (${ct})"
        } finally {
            cluster.clearFrontendDebugPoints()
        }

        cluster.checkFeIsAlive(1, true)

        def partitions = sql "SHOW PARTITIONS FROM ${tbl}"
        assert partitions.size() >= 3

        def now = new Date()
        def dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
        def today = dateFormat.format(now)
        sql "INSERT INTO ${tbl} VALUES (1, '${today} 12:00:00', 'alice')"
        def cnt = sql "SELECT count(*) FROM ${tbl}"
        assert cnt[0][0] == 1L : "expected 1 row, got ${cnt[0][0]}"
    }
}
