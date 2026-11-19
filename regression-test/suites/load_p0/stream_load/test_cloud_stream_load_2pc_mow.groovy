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

suite("test_cloud_stream_load_2pc_mow", "p0") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_cloud_stream_load_2pc_mow"
    sql """
        CREATE TABLE test_cloud_stream_load_2pc_mow (k INT NOT NULL, v INT, seq INT)
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("enable_unique_key_merge_on_write" = "true", "function_column.sequence_col" = "seq")
    """

    def load = { String rows, boolean twoPhase ->
        def txnId
        streamLoad {
            table 'test_cloud_stream_load_2pc_mow'
            set 'column_separator', ','
            set 'two_phase_commit', twoPhase.toString()
            inputStream new ByteArrayInputStream(rows.getBytes("UTF-8"))
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase(), result)
                assertEquals(2, json.NumberLoadedRows)
                txnId = json.TxnId
            }
        }
        return txnId
    }
    def finish = { txnId, String operation ->
        def command = ["curl", "--silent", "--show-error", "--location-trusted", "-X", "PUT",
                       "-u", "${context.config.feHttpUser}:${context.config.feHttpPassword}",
                       "-H", "txn_id:${txnId}", "-H", "txn_operation:${operation}",
                       "http://${context.config.feHttpAddress}/api/${context.dbName}/test_cloud_stream_load_2pc_mow/_stream_load_2pc"]
        def process = command.collect { it.toString() }.execute()
        def stdout = new StringBuilder()
        def stderr = new StringBuilder()
        process.waitForProcessOutput(stdout, stderr)
        assertEquals(0, process.exitValue(), stderr.toString())
        assertEquals("success", parseJson(stdout.toString()).status.toLowerCase(), stdout.toString())
    }
    def readRows = { sql "SELECT k, v, seq FROM test_cloud_stream_load_2pc_mow ORDER BY k" }

    def txnId = load("1,200,20\n2,200,10\n", true)
    assertEquals([], readRows())
    load("1,100,10\n2,100,20\n", false)
    assertEquals([[1, 100, 10], [2, 100, 20]], readRows())
    finish(txnId, "commit")
    def committedRows = [[1, 200, 20], [2, 100, 20]]
    assertEquals(committedRows, readRows())

    finish(load("1,300,30\n3,300,30\n", true), "abort")
    assertEquals(committedRows, readRows())

    setBeConfigTemporary([enable_stream_load_commit_txn_on_be: true]) {
        finish(load("1,400,40\n2,500,50\n", true), "commit")
        assertEquals([[1, 400, 40], [2, 500, 50]], readRows())
    }
}
