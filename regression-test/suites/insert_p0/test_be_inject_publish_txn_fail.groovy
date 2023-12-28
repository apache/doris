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

suite('test_be_inject_publish_txn_fail', 'nonConcurrent') {
    def tbl = 'test_be_inject_publish_txn_fail_tbl'
    def dbug1 = 'TxnManager.publish_txn.random_failed_before_save_rs_meta'
    def dbug2 = 'TxnManager.publish_txn.random_failed_after_save_rs_meta'

    def allBeReportTask = { ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
        backendId_to_backendIP.each { beId, beIp ->
            def port = backendId_to_backendHttpPort.get(beId) as int
            be_report_task(beIp, port)
        }
    }

    def testInsertValue = { dbug, value ->
        // insert succ but not visible
        GetDebugPoint().enableDebugPointForAllBEs(dbug, [percent : 1.0])
        sql "INSERT INTO ${tbl} VALUES (${value})"
        sleep(6000)
        order_qt_select_1 "SELECT * FROM ${tbl}"

        GetDebugPoint().disableDebugPointForAllBEs(dbug)

        // be report publish fail to fe, then fe will not remove its task.
        // after be report its tasks, fe will resend publish version task to be.
        // the txn will visible
        allBeReportTask()
        sleep(8000)
        order_qt_select_2 "SELECT * FROM ${tbl}"
    }

    try {
        sql "DROP TABLE IF EXISTS ${tbl} FORCE"
        sql "CREATE TABLE ${tbl} (k INT) DISTRIBUTED BY HASH(k) BUCKETS 5 PROPERTIES ('replication_num' = '1')"

        sql "ADMIN SET FRONTEND CONFIG ('agent_task_resend_wait_time_ms' = '1000')"
        sql 'SET insert_visible_timeout_ms = 2000'

        testInsertValue dbug1, 100
        testInsertValue dbug2, 200
    } finally {
        try {
            sql "ADMIN SET FRONTEND CONFIG ('agent_task_resend_wait_time_ms' = '5000')"
        } catch (Throwable e) {
        }

        try {
            GetDebugPoint().disableDebugPointForAllBEs(dbug1)
        } catch (Throwable e) {
        }

        try {
            GetDebugPoint().disableDebugPointForAllBEs(dbug2)
        } catch (Throwable e) {
        }

        sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    }
}
