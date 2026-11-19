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

suite("test_cloud_http_stream_2pc_mow", "p0") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_cloud_http_stream_2pc_mow"
    sql """
        CREATE TABLE test_cloud_http_stream_2pc_mow (k INT NOT NULL, v INT)
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES ("enable_unique_key_merge_on_write" = "true")
    """
    sql "INSERT INTO test_cloud_http_stream_2pc_mow VALUES (1, 10)"

    def txnId
    streamLoad {
        table 'test_cloud_http_stream_2pc_mow'
        set 'version', '1'
        set 'two_phase_commit', 'true'
        set 'sql', """
            INSERT INTO ${context.dbName}.test_cloud_http_stream_2pc_mow
            SELECT c1, c2 FROM http_stream("format"="csv", "column_separator"=",")
        """
        time 10000
        inputStream new ByteArrayInputStream("1,20\n2,30\n".getBytes("UTF-8"))
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
    assertTrue(txnId > 0, "HTTP stream must return the precommitted transaction ID")
    def readRows = { sql "SELECT k, v FROM test_cloud_http_stream_2pc_mow ORDER BY k" }
    assertEquals([[1, 10]], readRows())

    def command = ["curl", "--silent", "--show-error", "--location-trusted", "-X", "PUT",
                   "-u", "${context.config.feHttpUser}:${context.config.feHttpPassword}",
                   "-H", "txn_id:${txnId}", "-H", "txn_operation:commit",
                   "http://${context.config.feHttpAddress}/api/${context.dbName}/test_cloud_http_stream_2pc_mow/_stream_load_2pc"]
    def process = command.collect { it.toString() }.execute()
    def stdout = new StringBuilder()
    def stderr = new StringBuilder()
    process.waitForProcessOutput(stdout, stderr)
    assertEquals(0, process.exitValue(), stderr.toString())
    assertEquals("success", parseJson(stdout.toString()).status.toLowerCase(), stdout.toString())
    assertEquals([[1, 20], [2, 30]], readRows())
}
