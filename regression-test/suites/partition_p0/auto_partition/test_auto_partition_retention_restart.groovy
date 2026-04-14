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

suite("test_auto_partition_retention_restart", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    options.feConfigs += [
        "dynamic_partition_enable=true",
        "dynamic_partition_check_interval_seconds=1"
    ]

    docker(options) {
        def tableName = "test_auto_partition_retention_restart"

        def waitPartitions = { Closure<Boolean> predicate, long timeoutMs = 30000L ->
            long start = System.currentTimeMillis()
            while (System.currentTimeMillis() - start < timeoutMs) {
                def partitions = sql "show partitions from ${tableName}"
                if (predicate.call(partitions)) {
                    return partitions
                }
                sleep(1000)
            }
            def partitions = sql "show partitions from ${tableName}"
            assertTrue(predicate.call(partitions))
            return partitions
        }

        sql "drop table if exists ${tableName} force"
        sql """
            create table ${tableName}(
                k0 datetime(6) not null
            )
            auto partition by range (date_trunc(k0, 'day')) ()
            distributed by hash(`k0`) buckets 1
            properties(
                "replication_num" = "1"
            )
        """

        try {
            sql """
                insert into ${tableName}
                select date_add('2020-01-01 00:00:00', interval number day)
                from numbers("number" = "100")
            """

            sql "alter table ${tableName} set ('partition.retention_count' = '3')"

            def partitionsBeforeRestart = waitPartitions.call({ partitions -> partitions.size() == 3 })
            def oldestBeforeRestart = partitionsBeforeRestart[0][1].toString()

            cluster.restartFrontends()
            sleep(20000)
            context.reconnectFe()

            sql """
                insert into ${tableName}
                values ('2020-04-10 00:00:00')
            """

            def partitions = waitPartitions.call({ currentPartitions ->
                currentPartitions.size() == 3 && currentPartitions[0][1].toString() != oldestBeforeRestart
            })
            assertEquals(partitions.size(), 3)
            assertTrue(partitions[0][1].toString().startsWith("p20200408"))
            assertTrue(partitions[2][1].toString().startsWith("p20200410"))

            def rows = sql "select * from ${tableName} order by k0"
            assertEquals(rows.size(), 3)
            assertEquals(rows[0][0].toString().startsWith("2020-04-08"), true)
            assertEquals(rows[2][0].toString().startsWith("2020-04-10"), true)
        } finally {
            sql "drop table if exists ${tableName} force"
        }
    }
}
