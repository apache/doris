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
import com.mysql.cj.jdbc.StatementImpl
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType
import org.apache.doris.regression.suite.SuiteCluster

suite("test_create_partition_and_insert_overwrite_race", 'p0, docker') {
    if (!isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.enableDebugPoints()
    // one master, one observer
    options.setFeNum(2)
    options.feConfigs.add('sys_log_verbose_modules=org')
    options.setBeNum(3)
    options.cloudMode = true

    // 1. connect to observer
    options.connectToFollower = true
    docker(options) {
        sql """set enable_sql_cache=false"""
        def tbl = 'test_create_partition_and_insert_overwrite_race_tbl'
        def tbl2 = 'test_create_partition_and_insert_overwrite_race_tbl2'
        def createTableSql = { String tableName -> 
            sql """
                CREATE TABLE ${tableName} (
                    order_id    BIGINT,
                    create_dt   datetime,
                    username    VARCHAR(20)
                )
                DUPLICATE KEY(order_id)
                PARTITION BY RANGE(create_dt) ()
                DISTRIBUTED BY HASH(order_id) BUCKETS 10
                PROPERTIES(
                    "dynamic_partition.enable" = "true",
                    "dynamic_partition.time_unit" = "DAY",
                    "dynamic_partition.start" = "-5",
                    "dynamic_partition.end" = "5",
                    "dynamic_partition.prefix" = "p",
                    "dynamic_partition.create_history_partition" = "true"
                );
                """
        }

        createTableSql(tbl)
        createTableSql(tbl2)

        // Generate insert statements with dates: current date -2, -1, 0, +1, +2 days
        def now = new Date()
        def dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
        for (def i = -2; i <= 2; i++) {
            def targetDate = new Date(now.time + i * 24 * 60 * 60 * 1000L)
            def dateStr = dateFormat.format(targetDate)
            def hour = String.format("%02d", Math.abs(i) + 1)
            def insertDate = "${dateStr} ${hour}:00:00"
            sql """insert into ${tbl2} values (${i + 3}, '${insertDate}', 'test')"""
        }

        sql """DROP TABLE ${tbl}"""
        def partitionNameFormat = new java.text.SimpleDateFormat("yyyyMMdd")
        def currentPartitionName = "p" + partitionNameFormat.format(now)
        cluster.injectDebugPoints(NodeType.FE, ['FE.logAddPartition.slow':[pName:currentPartitionName, sleep:50 * 1000]])
        def futrue = thread {
            for (def i = 0; i < 55; i++) {
                try_sql """INSERT OVERWRITE TABLE ${tbl} partition(*) select * from ${tbl2}"""
                sleep(1 * 1000)
                cluster.checkFeIsAlive(2, true)
            }
        }
        def future1 = thread {
            createTableSql(tbl) 
        }
        futrue.get()
        future1.get()
    }
}
