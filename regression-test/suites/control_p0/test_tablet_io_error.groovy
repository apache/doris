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

import groovy.io.FileType

suite('test_tablet_io_error', 'docker') {
    def runTest = { isRead, debugPointName, isDropTabletDir ->
        GetDebugPoint().clearDebugPointsForAllBEs()
        def tbl =  'tbl_test_tablet_io_error'
        sql "create table ${tbl} (k int) distributed by hash(k) buckets 1 properties('replication_num' = '2')"
        sql "insert into ${tbl} values (1)"
        sql "insert into ${tbl} values (2)"
        sql "insert into ${tbl} values (3)"
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
        assertEquals(2, tablets.size())
        def tabletId = tablets[0].TabletId.toLong()
        def injectBe = cluster.getBeByBackendId(tablets[0].BackendId.toLong())
        assertNotNull(injectBe)

        sql 'set use_fix_replica = 0'

        def tabletOnInjectBe = sql_return_maparray("SHOW TABLETS FROM ${tbl}").find { it.BackendId.toLong() == injectBe.backendId }
        assertNotNull(tabletOnInjectBe)

        if (debugPointName != null) {
            GetDebugPoint().enableDebugPoint(injectBe.host, injectBe.httpPort, injectBe.getNodeType(),
                    debugPointName, [ sub_path : "/${tabletId}/" ])
        }

        if (isDropTabletDir) {
            // the docker files owner is root, need change its permission
            cluster.addRWPermToAllFiles()
            def dataPath = new File("${injectBe.path}/storage/1.HDD/data")
            dataPath.eachFile(FileType.DIRECTORIES) { shardPath ->
                shardPath.eachFile(FileType.DIRECTORIES) { tabletPath ->
                    try {
                        if (tabletPath.getName().toLong() == tabletId) {
                            logger.info("delete tablet path: ${tabletPath}")
                            tabletPath.deleteDir()
                        }
                    } catch (Throwable t) {
                        logger.warn('delete tablet path exception: ', t)
                    }
                }
            }
        }

        boolean hasExcept = false
        try {
            if (isRead) {
                sql "select * from ${tbl}"
            } else {
                sql "insert into ${tbl} values (1)"
            }
        } catch (Throwable e) {
            logger.info("exec exeption: ${e.getMessage()}")
            hasExcept = true
        }
        assertTrue(hasExcept)

        sleep 8000

        // be will report tablet as bad, then fe will drop it
        tabletOnInjectBe = sql_return_maparray("SHOW TABLETS FROM ${tbl}").find { it.BackendId.toLong() == injectBe.backendId }
        assertNull(tabletOnInjectBe)
        sql "insert into ${tbl} values (1)"
        sql "select * from ${tbl}"

        sql "drop table ${tbl} force"
    }

    def options = new ClusterOptions()
    options.cloudMode = false
    options.enableDebugPoints()
    options.feConfigs += [
        'disable_balance=true',
        'tablet_checker_interval_ms=500',
        'schedule_batch_size=1000',
        'schedule_slot_num_per_hdd_path=1000',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'max_tablet_io_errors=1',
        'disable_page_cache=true',
    ]

    docker(options) {
        runTest(true, 'LocalFileReader::read_at_impl.io_error', false)
        runTest(true, 'LocalFileSystem.create_file_impl.open_file_failed', false)
        runTest(true, null, true)
        runTest(false, 'LocalFileWriter::appendv.io_error', false)
        runTest(false, 'LocalFileSystem.create_file_impl.open_file_failed', false)
        runTest(false, null, true)
    }
}
