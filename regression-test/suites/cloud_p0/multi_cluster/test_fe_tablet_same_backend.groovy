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

suite('test_fe_tablet_same_backend', 'multi_cluster,docker') {
    def tbl1 = 'tbl_1_test_fe_tablet_same_backend'
    def tbl2 = 'tbl_2_test_fe_tablet_same_backend'
    def bucketNum = 6

    def choseDeadBeIndex = 1

    def getTabletBeIdsForEachFe = { tbl, isPrimaryBe ->
        def result = []
        def frontends = cluster.getAllFrontends()
        for (def fe : frontends) {
            def feUrl = "jdbc:mysql://${fe.host}:${fe.queryPort}/?useLocalSessionState=false&allowLoadLocalInfile=false"
            feUrl = context.config.buildUrlWithDb(feUrl, context.dbName)
            connect('root', '', feUrl) {
                    sql 'SET forward_to_master=false'
                    sql "SELECT * FROM ${tbl}"
                    def tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
                    result.add(tablets.collectEntries {
                        def tabletId = it.TabletId as long
                        def backendId = (isPrimaryBe ? it.PrimaryBackendId : it.BackendId) as long
                        [tabletId, backendId]
                    })
            }
        }
        return result
    }

    def checkOneTable = { tbl, isColocateTbl, isAllBeAliveOrDeadLong, isAwaiting ->
        def feToCurBeIds = getTabletBeIdsForEachFe(tbl, false)
        def feToPrimaryBeIds = getTabletBeIdsForEachFe(tbl, true)
        def succ = true

        logger.info('check table got: cur backends {}, primary backends {}, isColocateTbl {}, isAllBeAliveOrDeadLong {}',
            feToCurBeIds, feToPrimaryBeIds, isColocateTbl, isAllBeAliveOrDeadLong)

        // check whether 3 frontends are consistent
        for (def feToBeIds : [ feToCurBeIds, feToPrimaryBeIds ]) {
            assertEquals(3, feToBeIds.size())
            for (def tablets : feToBeIds) {
                assertEquals(bucketNum, tablets.size())
            }
            for (def i = 1; i <= 2; i++) {
                if (feToBeIds[0] != feToBeIds[i]) {
                    succ = false
                    if (!isAwaiting) {
                        assertEquals(feToBeIds[0], feToBeIds[i],
                                    "3 fe inconsistent backends: 3 fe to backends ${feToCurBeIds}, isColocateTbl ${isColocateTbl}")
                    }
                }
            }
        }

        // check whether primary be ids equals to current be ids,
        def chosenBe = cluster.getBeByIndex(choseDeadBeIndex)
        def primaryTabletNum = 0
        feToPrimaryBeIds[0].each { if (it.value == chosenBe.backendId) { primaryTabletNum++ } }
        if (isColocateTbl) {
            assertEquals(feToPrimaryBeIds[0], feToCurBeIds[0])
            assertEquals(chosenBe.alive ? 2 : 0, primaryTabletNum)
            } else {
            if (isAllBeAliveOrDeadLong) {
                assertEquals(feToPrimaryBeIds[0], feToCurBeIds[0])
                } else {
                assertNotEquals(feToPrimaryBeIds[0], feToCurBeIds[0])
            }
            assertEquals(chosenBe.alive || !isAllBeAliveOrDeadLong ? 2 : 0, primaryTabletNum)
        }

        def curTabletNum = 0
        feToCurBeIds[0].each { if (it.value == chosenBe.backendId) { curTabletNum++ } }
        assertEquals(chosenBe.alive ? 2 : 0, curTabletNum)

        return succ
    }

    def checkAllTableImpl = { isAllBeAliveOrDeadLong, isAwaiting ->
        return checkOneTable(tbl1, false, isAllBeAliveOrDeadLong, isAwaiting)
                    && checkOneTable(tbl2, true, isAllBeAliveOrDeadLong, isAwaiting)
    }

    def checkAllTable = { isAllBeAliveOrDeadLong ->
        dockerAwaitUntil(60) {
            checkAllTableImpl(isAllBeAliveOrDeadLong, true)
        }
        checkAllTableImpl(isAllBeAliveOrDeadLong, false)
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'enable_cloud_warm_up_for_rebalance=true',
        'cloud_tablet_rebalancer_interval_second=1',
        'cloud_balance_tablet_percent_per_run=1.0',
    ]
    options.setFeNum(3)
    options.setBeNum(3)
    options.cloudMode = true
    options.enableDebugPoints()

    docker(options) {
        sql "ADMIN SET ALL FRONTENDS CONFIG ('rehash_tablet_after_be_dead_seconds' = '3600')"

        sql "CREATE TABLE ${tbl1} (a INT) DISTRIBUTED BY HASH(a) BUCKETS ${bucketNum}"
        sql "CREATE TABLE ${tbl2} (a INT) DISTRIBUTED BY HASH(a) BUCKETS ${bucketNum} PROPERTIES ('colocate_with' = 'foo')"
        sql "INSERT INTO ${tbl1} VALUES (1)"
        sql "INSERT INTO ${tbl2} VALUES (1)"

        // all fe alive
        checkAllTable(true)

        cluster.stopBackends(choseDeadBeIndex)
        dockerAwaitUntil(60) {
            def chosenBe = cluster.getBeByIndex(choseDeadBeIndex)
            !chosenBe.alive
        }

        // be-1 dead, but not dead for a long time
        checkAllTable(false)

        sql "ADMIN SET ALL FRONTENDS CONFIG ('rehash_tablet_after_be_dead_seconds' = '1')"

        sleep(2 * 1000)
        // be-1 dead, and dead for a long time
        checkAllTable(true)

        def choseRestartFeIndex = cluster.getOneFollowerFe().index
        cluster.stopFrontends(choseRestartFeIndex)
        dockerAwaitUntil(60) {
            def chosenFe = cluster.getFeByIndex(choseRestartFeIndex)
            !chosenFe.alive
        }

        cluster.startFrontends(choseRestartFeIndex)
        dockerAwaitUntil(60) {
            def chosenFe = cluster.getFeByIndex(choseRestartFeIndex)
            chosenFe.alive
        }

        def frontends = cluster.getAllFrontends()
        for (def fe : frontends) {
            def feUrl = "jdbc:mysql://${fe.host}:${fe.queryPort}/?useLocalSessionState=false&allowLoadLocalInfile=false"
            feUrl = context.config.buildUrlWithDb(feUrl, context.dbName)
            connect('root', '', feUrl) {
                sql 'SET forward_to_master=false'
                sql "ADMIN SET FRONTEND CONFIG ('rehash_tablet_after_be_dead_seconds' = '1')"
            }
        }

        // be-1 dead, and dead for a long time
        checkAllTable(true)
    }
}
