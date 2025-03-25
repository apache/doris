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
import org.apache.doris.regression.suite.SuiteCluster

class InjectCase {

    boolean writeMayFailed
    Map<String, Map<String, String>> fePoints
    Map<String, Map<String, String>> bePoints

    InjectCase(boolean writeMayFailed, Map<String, Map<String, String>> fePoints,
        Map<String, Map<String, String>> bePoints) {
        this.writeMayFailed = writeMayFailed
        this.fePoints = fePoints
        this.bePoints = bePoints
        }

}

suite('test_min_load_replica_num_complicate', 'docker') {
    def beCloneCostMs = 3000

    def random = new Random()
    def insertThreadNum = getSuiteConf('insertThreadNum', '3') as int
    def iterationNum = getSuiteConf('iterationNum', '2') as int
    def injectProb = getSuiteConf('injectProb', '100') as int  // inject perc = injectProb / 100
    def insertBatchSize = 4
    def bucketNum = 2

    def run = { int replicaNum, int minLoadNum ->
        logger.info("start run test_min_load_replica_num_complicate with replica num ${replicaNum}, "
                + "min load replica num ${minLoadNum}, iterationNum ${iterationNum}, injectProb ${injectProb} %")

        def allInjectCases = [
            new InjectCase(true,
                ['OlapTableSink.write_random_choose_sink' : [needCatchUp:false, sinkNum: minLoadNum]],
                ['EngineCloneTask.wait_clone' : [duration:beCloneCostMs]]
            ),
            new InjectCase(false,
                ['PublishVersionDaemon.stop_publish' : [timeout:10]],
                ['EngineCloneTask.wait_clone' : [duration:beCloneCostMs]]
            ),
            new InjectCase(false,
                ['PublishVersionDaemon.not_wait_unfinished_tasks' : null],
                ['EngineCloneTask.wait_clone' : [duration:beCloneCostMs]]
            ),
            new InjectCase(true,
                [:],
                ['TxnManager.commit_txn_random_failed' : null]
            ),
        ]

        def options = new ClusterOptions()
        options.enableDebugPoints()
        options.feConfigs.add('disable_balance=true')
        options.feConfigs.add('tablet_checker_interval_ms=1000')
        options.beNum = replicaNum + 1

        def tbl = "test_x_load_complicate_replica_${replicaNum}_x_${minLoadNum}"
        tbl = tbl.replaceAll('-', 'm')

        docker(options) {
            sql """
                CREATE TABLE ${tbl}
                (
                    k1 int
                )
                DISTRIBUTED BY HASH(`k1`) BUCKETS ${bucketNum}
                PROPERTIES (
                   "replication_num" = "${replicaNum}",
                   "min_load_replica_num" = "${minLoadNum}"
                );
            """

            // wait visible 10 min
            sql "SET GLOBAL insert_visible_timeout_ms = ${1000 * 60 * 10}"

            def rowIndex = 0
            def getNextBatch = {
                def rows = []
                for (def k = 0; k < insertBatchSize; k++) {
                    rows.add('(' + rowIndex + ')')
                    rowIndex++
                }
                return rows.join(', ')
            }

            def insertSuccNum = 0
            for (def i = 0; i < iterationNum; i++) {
                def futures = []
                boolean writeMayFailed = false
                if (random.nextInt(100) < injectProb) {
                    sql '''admin set frontend config ("disable_tablet_scheduler" = "true")'''

                    def originBackends = cluster.getBackends()
                    cluster.addBackend(1)

                    def injectCase = allInjectCases[random.nextInt(allInjectCases.size())]
                    writeMayFailed = injectCase.writeMayFailed
                    cluster.injectDebugPoints(NodeType.FE, injectCase.fePoints)
                    cluster.injectDebugPoints(NodeType.BE, injectCase.bePoints)

                    futures.add(thread {
                        sql '''admin set frontend config ("disable_tablet_scheduler" = "false")'''
                        cluster.decommissionBackends(clean = true, originBackends.get(0).index)
                        cluster.clearFrontendDebugPoints()
                        cluster.clearBackendDebugPoints()

                        return false
                    })
                }

                for (def j = 0; j < insertThreadNum; j++) {
                    def values = getNextBatch()
                    futures.add(thread {
                        try {
                            sql "INSERT INTO ${tbl} VALUES ${values}"
                        } catch (Exception e) {
                            if (!writeMayFailed) {
                                throw e
                            }
                            return false
                        }
                        return true
                    })
                }

                futures.each { if (it.get()) { insertSuccNum++ } }
            }

            sql """ ALTER TABLE ${tbl} SET ( "min_load_replica_num" = "-1" ) """

            for (def i = 0; i < 5; i++) {
                sql "INSERT INTO ${tbl} VALUES ${getNextBatch()}"
                insertSuccNum++
            }

            def rowNum = insertSuccNum * insertBatchSize
            for (def i = 0; i < replicaNum; i++) {
                sql "set use_fix_replica = ${i}"
                def result = sql "select count(*) from ${tbl}"
                assertEquals(rowNum, result[0][0] as int)
            }

            sql 'set use_fix_replica = -1'
            def versionNum = insertSuccNum + 1
            def result = sql "show tablets from ${tbl}"
            assertEquals(bucketNum * replicaNum, result.size())
            for (def tablet : result) {
                assertEquals(versionNum, tablet[5] as int)
            }
        }
    }

    run(2, 1)
    run(2, -1)
    run(3, 1)
    run(3, -1)
}
