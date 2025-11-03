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

import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite('test_schema_change_fail', 'p0,p2,nonConcurrent') {
    if (isCloudMode()) {
        return
    }

    def frontends = sql_return_maparray('show frontends')
    def backends = sql_return_maparray('show backends')
    def forceReplicaNum = getFeConfig('force_olap_table_replication_num').toInteger()
    if (frontends.size() < 2 || backends.size() < 3 || forceReplicaNum == 1 || forceReplicaNum == 2) {
        return
    }

    def tbl = 'test_schema_change_fail'
    def injectName = 'SchemaChangeJob.process_alter_tablet.alter_fail'
    def injectBe = null

    def checkReplicaBad = { ->
        def tabletId = sql_return_maparray("SHOW TABLETS FROM ${tbl}")[0].TabletId.toLong()
        def replicas = sql_return_maparray(sql_return_maparray("SHOW TABLET ${tabletId}").DetailCmd)
        int badReplicaNum = 0
        for (def replica : replicas) {
            if (replica.BackendId.toLong() == injectBe.BackendId.toLong()) {
                assertEquals(true, replica.IsBad.toBoolean())
                badReplicaNum++
            } else {
                assertEquals(false, replica.IsBad.toBoolean())
            }
        }
        assertEquals(1, badReplicaNum)
    }

    def followFe = frontends.stream().filter(fe -> !fe.IsMaster.toBoolean()).findFirst().orElse(null)
    def followFeUrl =  "jdbc:mysql://${followFe.Host}:${followFe.QueryPort}/?useLocalSessionState=false&allowLoadLocalInfile=false"
    followFeUrl = context.config.buildUrlWithDb(followFeUrl, context.dbName)

    try {
        setFeConfig('disable_tablet_scheduler', true)

        sleep(3000)

        sql "DROP TABLE IF EXISTS ${tbl} FORCE"
        sql """
            CREATE TABLE ${tbl}
            (
                `a` TINYINT NOT NULL,
                `b` TINYINT NULL
            )
            UNIQUE KEY (`a`)
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES
            (
                'replication_num' = '${backends.size()}',
                'light_schema_change' = 'false'
            )
        """

        def injectBeId = sql_return_maparray("SHOW TABLETS FROM ${tbl}")[0].BackendId.toLong()
        injectBe = backends.stream().filter(be -> be.BackendId.toLong() == injectBeId).findFirst().orElse(null)
        assertNotNull(injectBe)

        sql "INSERT INTO ${tbl} VALUES (1, 2), (3, 4)"

        DebugPoint.enableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, injectName)
        sql "ALTER TABLE ${tbl} MODIFY COLUMN b DOUBLE"
        sleep(5 * 1000)

        def jobs = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName = '${tbl}' ORDER BY CreateTime DESC LIMIT 1"
        assertEquals(1, jobs.size())
        assertEquals('FINISHED', jobs[0].State)

        checkReplicaBad()
        connect('root', '', followFeUrl) {
            checkReplicaBad()
        }
    } finally {
        setFeConfig('disable_tablet_scheduler', false)
        if (injectBe != null) {
            DebugPoint.disableDebugPoint(injectBe.Host, injectBe.HttpPort.toInteger(), NodeType.BE, injectName)
        }
        sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    }
}
