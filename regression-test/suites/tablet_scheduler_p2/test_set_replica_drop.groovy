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

suite('test_set_replica_drop') {
    def forceReplicaNum = getFeConfig('force_olap_table_replication_num') as int
    if (forceReplicaNum > 0 && forceReplicaNum != 1) {
        return
    }

    def backends = sql 'show backends'
    if (backends.size() < 2) {
        return
    }

    def config = [
        disable_balance : true,
        schedule_slot_num_per_ssd_path : 1000,
        schedule_slot_num_per_hdd_path : 1000,
        schedule_batch_size: 1000,
    ]

    setFeConfigTemporary(config) {
        def tbl = 'test_set_replica_drop'
        sql "DROP TABLE IF EXISTS ${tbl} FORCE"
        sql "CREATE TABLE ${tbl} (k int) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
        sql "INSERT INTO ${tbl} VALUES (1), (2), (3)"
        sql "SELECT * FROM ${tbl}"

        def tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
        assertEquals(1, tablets.size())
        def oldTablet = tablets[0]

        sql """
            ADMIN SET REPLICA STATUS PROPERTIES(
                'tablet_id' = '${oldTablet.TabletId}',
                'backend_id' = '${oldTablet.BackendId}',
                'status' = 'drop'
            )"""

        def maxWaitSeconds = 300
        for (def i = 0; i < maxWaitSeconds; i++) {
            sql "SELECT * FROM ${tbl}"
            def ok = true
            def end = i == maxWaitSeconds - 1
            tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
            if (tablets.size() != 1) {
                ok = false
                if (end) {
                    assertEquals(1, tablets.size())
                }
            } else {
                def newTablet = tablets[0]
                if (newTablet.BackendId == oldTablet.BackendId) {
                    ok = false
                    if (end) {
                        assertTrue(newTablet.BackendId != oldTablet.BackendId)
                    }
                }
            }

            if (ok) {
                break
            } else {
                sleep 1000
            }
        }

        sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    }
}
