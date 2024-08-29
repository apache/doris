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
import org.apache.doris.regression.util.Http

suite('test_report_tablet_batch', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.enableDebugPoints()
    options.feConfigs += [
        'disable_tablet_scheduler=true',
        'disable_balance=true',
        'tablet_checker_interval_ms=500',
        'schedule_batch_size=1000',
        'schedule_slot_num_per_hdd_path=1000',
    ]
    options.beConfigs += [ 'report_tablet_interval_seconds=1' ]
    docker(options) {
        def wait = { -> sleep(5000) }

        def injectBe = cluster.getBeByIndex(1)

        def injectTabletReport = { injectParam ->
            GetDebugPoint().enableDebugPoint(injectBe.host, injectBe.httpPort, injectBe.getNodeType(),
                    'Tablet.build_tablet_report_info', injectParam)
            wait()
        }

        def setEnableTabletReport = { enable ->
            if (enable) {
                GetDebugPoint().disableDebugPoint(injectBe.host, injectBe.httpPort, injectBe.getNodeType(),
                        'task_worker_pool.report_tablet.stop')
            } else {
                GetDebugPoint().enableDebugPoint(injectBe.host, injectBe.httpPort, injectBe.getNodeType(),
                        'task_worker_pool.report_tablet.stop')
            }
        }

        def prepareTable = { tbl, replication_num = 3 ->
            setFeConfig 'disable_tablet_scheduler', true
            setEnableTabletReport true
            sql "create table ${tbl} (k int) distributed by hash(k) buckets 6 properties('replication_num' = '${replication_num}')"
            sql "insert into ${tbl} values (11)"
            sql "insert into ${tbl} values (12)"
            sql "insert into ${tbl} values (13)"
            sql "insert into ${tbl} values (14)"
            sql "insert into ${tbl} values (15)"
            sql "insert into ${tbl} values (16)"
        }

        def checkTablets = { tbl, m = [:] ->
            def checker = m.getOrDefault('checker', null)
            def isBad = m.getOrDefault('isBad', false)
            def version = m.getOrDefault('version', 7L)
            def lstSuccVersion = m.getOrDefault('lstSuccVersion', 7L)
            def lstFailedVersion = m.getOrDefault('lstFailedVersion', -1L)
            def frontends = cluster.getAllFrontends()
            for (def fe : frontends) {
                def feUrl = "jdbc:mysql://${fe.host}:${fe.queryPort}/?useLocalSessionState=false&allowLoadLocalInfile=false"
                feUrl = context.config.buildUrlWithDb(feUrl, context.dbName)
                connect('root', '', feUrl) {
                    sql 'SET forward_to_master=false'
                    def tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
                    def targetTablets = tablets.findAll { it.BackendId.toLong() == injectBe.backendId }
                    if (checker != null) {
                        checker(tablets, targetTablets)
                        return
                    }

                    assertEquals(18, tablets.size())
                    assertEquals(6, targetTablets.size())
                    for (def tablet : targetTablets) {
                        logger.info("got target tablet from ${fe.isMaster ? 'master fe' : 'follower fe'}: ${tablet}")
                        if (isBad) {
                            assertTrue(tablet.IsBad.toBoolean())
                        } else {
                            assertFalse(tablet.IsBad.toBoolean())
                            assertEquals(version, tablet.Version.toLong())
                            assertEquals(lstSuccVersion, tablet.LstSuccessVersion.toLong())
                            assertEquals(lstFailedVersion, tablet.LstFailedVersion.toLong())
                        }
                    }
                }
            }
        }

        def getTablets = { tbl ->
            return sql_return_maparray("SHOW TABLETS FROM ${tbl}")
                    .collect { it.TabletId.toLong() }
                    .unique()
        }

        def getBeTablets = { beHost, beHttpPort ->
            return Http.GET("http://${beHost}:${beHttpPort}/tablets_json?limit=all", true)
                    .data.tablets.collect { it.tablet_id as long }
        }

        GetDebugPoint().enableDebugPointForAllFEs('Replica.regressive_version_immediately')

        // test tablets filter by TabletInvertIndex::needSync
        def tbl = 'tbl_for_need_sync'
        def testSync = { make_tablet_on_fe_unhealth ->
            prepareTable(tbl)
            checkTablets(tbl)

            make_tablet_on_fe_unhealth()

            // after be report ok, tablet on fe will be synced, and will be good
            injectTabletReport(null)
            checkTablets(tbl)

            sql "drop table if exists ${tbl} force"
        }
        // sync 1: be report version = 3L smaller, fe will not down its version, but set lstFailedVersion
        testSync {
            injectTabletReport(getTablets(tbl).collectEntries { ["tablet_${it}_version" , 3L] })
            checkTablets(tbl, [ version : 7L, lstSuccVersion : 7L, lstFailedVersion : 8L ])
        }
        // sync 2: be report replica bad
        testSync {
            injectTabletReport(getTablets(tbl).collectEntries { ["tablet_${it}_bad" , true] })
            checkTablets(tbl, [ isBad : true ])
        }

        // test tablets filter by TabletInvertIndex::needRecover
        tbl = 'tbl_for_need_recovery'
        def testRecover = { make_tablet_on_fe_unhealth ->
            setFeConfig 'disable_tablet_scheduler', true
            setEnableTabletReport true

            prepareTable(tbl)
            checkTablets(tbl)

            make_tablet_on_fe_unhealth()

            // be don't report and make fe scheduler repair them
            setEnableTabletReport false
            sleep 500
            injectTabletReport([:])
            setFeConfig 'disable_tablet_scheduler', false
            wait()
            checkTablets(tbl)

            sql "drop table if exists ${tbl} force"
        }
        // recovery 1: be report replica bad
        testRecover {
            injectTabletReport(getTablets(tbl).collectEntries { ["tablet_${it}_bad" , true] })
            checkTablets(tbl, [ isBad : true])
        }
        // recovery 2: be report version miss
        testRecover {
            injectTabletReport(getTablets(tbl).collectEntries { ["tablet_${it}_version_miss" , true] })
            checkTablets(tbl, [ lstFailedVersion : 8L])
        }
        // recovery 3: be report version smaller
        testRecover {
            injectTabletReport(getTablets(tbl).collectEntries { ["tablet_${it}_version" , 3L] })
            checkTablets(tbl, [ version : 7L, lstSuccVersion : 7L, lstFailedVersion : 8L])
        }

        // test deleted tablets filter by (fe - be)
        tbl = 'tbl_for_delete_on_fe_not_on_be'
        def testDeleteOnFeNotOnBe = { make_tablet_on_fe_unhealth ->
            setFeConfig 'disable_tablet_scheduler', true
            setEnableTabletReport true

            make_tablet_on_fe_unhealth()

            sql "drop table ${tbl} force"
        }
        // delete (fe - be) 1: 3 replica lost 1 replica, fe will just delete it
        prepareTable(tbl)
        testDeleteOnFeNotOnBe {
            checkTablets(tbl)

            injectTabletReport(getTablets(tbl).collectEntries { ["tablet_${it}_lost" , true] })

            // fe will just drop the lost tablets
            checkTablets(tbl, [ checker : { tablets, targetTablets ->
                assertEquals(12, tablets.size())
                assertEquals(0, targetTablets.size())
            }])
        }
        // delete (fe - be) 2: 1 replica lost 1 replica, fe will mark it as bad
        prepareTable(tbl, 1)
        testDeleteOnFeNotOnBe {
            checkTablets(tbl, [ checker : { tablets, targetTablets ->
                assertEquals(6, tablets.size())
                assertEquals(2, targetTablets.size())
                for (def tablet : targetTablets) {
                    assertFalse(tablet.IsBad.toBoolean())
                }
            }])

            injectTabletReport(getTablets(tbl).collectEntries { ["tablet_${it}_lost" , true] })

            // fe will just set the lost tablets as bad
            checkTablets(tbl, [ checker : { tablets, targetTablets ->
                assertEquals(6, tablets.size())
                assertEquals(2, targetTablets.size())
                for (def tablet : targetTablets) {
                    assertTrue(tablet.IsBad.toBoolean())
                }
            }])
        }

        // test deleted tablets filter by (be - fe)
        tbl = 'tbl_for_delete_on_be_not_on_fe'
        def testDeleteOnBeNotOnFe = { beReportTablet, make_tablet_on_fe_unhealth ->
            prepareTable(tbl)
            def feTablets = getTablets(tbl)
            def beTablets = getBeTablets(injectBe.host, injectBe.httpPort)
            logger.info("feTablets: ${feTablets}, beTablets: ${beTablets}")
            for (def tablet : feTablets) {
                assertTrue(beTablets.contains(tablet))
            }
            setEnableTabletReport beReportTablet
            sleep 2000
            GetDebugPoint().enableDebugPointForAllFEs('FE.HIGH_LOAD_BE_ID', [value : injectBe.backendId])
            setFeConfig 'disable_tablet_scheduler', false
            sql "ALTER TABLE ${tbl} MODIFY PARTITION(*) SET ('replication_num' = '2')"
            wait()
            checkTablets(tbl, [ checker : { tablets, targetTablets ->
                assertEquals(12, tablets.size())
                assertEquals(0, targetTablets.size())
            }])

            make_tablet_on_fe_unhealth()

            sql "drop table ${tbl} force"
        }
        // delete (be - fe) 1:  replica is reduant, fe/be will drop reduant tablets
        testDeleteOnBeNotOnFe(true) {
            def feTablets = getTablets(tbl)
            def beTablets = getBeTablets(injectBe.host, injectBe.httpPort)
            logger.info("feTablets: ${feTablets}, beTablets: ${beTablets}")
            for (def tablet : feTablets) {
                // since be report tablets, fe will let be delete them
                assertFalse(beTablets.contains(tablet))
            }
        }
        // delete (be - fe) 2:  replica is not engough, fe will add replicas
        testDeleteOnBeNotOnFe(false) {
            def feTablets = getTablets(tbl)
            def beTablets = getBeTablets(injectBe.host, injectBe.httpPort)
            logger.info("feTablets: ${feTablets}, beTablets: ${beTablets}")
            for (def tablet : feTablets) {
                // since be not report tablets, even fe delete them, be will not deleted
                assertTrue(beTablets.contains(tablet))
            }

            setFeConfig 'disable_tablet_scheduler', true
            cluster.stopBackends(2)
            wait()
            cluster.checkBeIsAlive(2, false)

            // disable scheduler, tablet not migrate
            checkTablets(tbl, [ checker : { tablets, targetTablets ->
                assertEquals(12, tablets.size())
                assertEquals(0, targetTablets.size())
            }])

            setEnableTabletReport true
            wait()
            // fe will add back replica since not engough replicas.
            checkTablets(tbl, [ checker :  { tablets, targetTablets ->
                assertEquals(18, tablets.size())
                assertEquals(6, targetTablets.size())
            }])
        }
    }
}
