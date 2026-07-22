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

suite("test_kill_query_by_connection_id", "p0") {
    // Run a long query in the background so we can kill it by connection ID.
    // Use the framework's thread{} helper so that unexpected exceptions in the
    // background thread are properly propagated (raw Thread.start + join() would
    // swallow them).
    def longQueryThread = thread {
        try {
            sql "SELECT sleep(60)"
        } catch (Exception e) {
            // expected: the query will be cancelled via KILL QUERY
        }
    }

    // Give the query time to appear in SHOW PROCESSLIST
    Thread.sleep(1000)

    // Find the connection ID of the long-running query
    def processList = sql_return_maparray "SHOW PROCESSLIST"
    def target = processList.find { it.Info?.contains("sleep(60)") }
    assertNotNull(target, "long query should appear in SHOW PROCESSLIST")
    def connId = target.Id

    // Cancel by connection ID (the code path fixed in this PR)
    sql "KILL QUERY ${connId}"

    // Wait for the background thread to exit
    longQueryThread.get()

    // Verify the query is no longer in PROCESSLIST
    def afterKill = sql_return_maparray "SHOW PROCESSLIST"
    def stillRunning = afterKill.find { it.Id == connId && it.Info?.contains("sleep(60)") }
    assertNull(stillRunning, "query should have been killed and removed from PROCESSLIST")
}
