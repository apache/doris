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

import java.util.concurrent.TimeUnit

suite("test_stream_load_2pc_owner", "p0,auth_call") {
    String owner = "test_stream_load_2pc_owner_user"
    String other = "test_stream_load_2pc_other_user"
    String password = "C123_567p"
    String db = "test_stream_load_2pc_owner_db"
    String feAddress = context.config.feHttpAddress
    String dataFile = "${context.file.parent}/../../data/auth_call/stream_load_data.csv"
    String computeGroup = ""

    // Exercise the authenticated FE entry and its actual BE redirect in both deployment modes.
    def authConfig = sql "ADMIN SHOW FRONTEND CONFIG LIKE 'enable_all_http_auth'"
    assertEquals("true", authConfig[0][1].toString().toLowerCase())
    if (isCloudMode()) {
        computeGroup = (sql "SHOW CLUSTERS")[0][0].toString()
    }

    def curl = { List args ->
        def process = (["curl", "--noproxy", "*", "--silent", "--show-error",
                        "--location-trusted", "--max-time", "120"] + args).collect { it.toString() }.execute()
        def stdout = new ByteArrayOutputStream()
        def stderr = new ByteArrayOutputStream()
        def outThread = process.consumeProcessOutputStream(stdout)
        def errThread = process.consumeProcessErrorStream(stderr)
        boolean finished = process.waitFor(150, TimeUnit.SECONDS)
        if (!finished) {
            process.destroyForcibly()
            process.waitFor()
        }
        outThread.join()
        errThread.join()
        assertTrue(finished, "curl timed out")
        assertEquals(0, process.exitValue(), stderr.toString("UTF-8"))
        return parseJson(stdout.toString("UTF-8"))
    }
    def headers = {
        return computeGroup.isEmpty() ? [] : ["-H", "cloud_cluster:${computeGroup}"]
    }
    def prepare = { boolean sqlHttp, String label ->
        def args = ["-u", "${owner}:${password}", "-H", "two_phase_commit:true",
                    "-H", "label:${label}", "-H", "column_separator:,"] + headers()
        if (sqlHttp) {
            args.addAll(["-H", "sql:insert into ${db}.test_stream_load_2pc_owner "
                    + "select c1, c2 from http_stream(\"format\"=\"csv\",\"column_separator\"=\",\")",
                    "-T", dataFile, "http://${feAddress}/api/_http_stream"])
        } else {
            args.addAll(["-T", dataFile,
                    "http://${feAddress}/api/${db}/test_stream_load_2pc_owner/_stream_load"])
        }
        def result = curl(args)
        assertEquals("Success", result.Status)
        assertTrue(result.TwoPhaseCommit.toString().equalsIgnoreCase("true"))
        return result
    }
    def finish = { String user, def txn, boolean byLabel, String operation ->
        def key = byLabel ? "label:${txn.Label}" : "txn_id:${txn.TxnId}"
        return curl(["-X", "PUT", "-u", "${user}:${password}", "-H", key,
                "-H", "txn_operation:${operation}"] + headers()
                + ["http://${feAddress}/api/${db}/_stream_load_2pc"])
    }

    try {
        [owner, other].each { user ->
            try_sql "DROP USER ${user}"
            sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"
            if (!computeGroup.isEmpty()) {
                sql "GRANT USAGE_PRIV ON CLUSTER `${computeGroup}` TO ${user}"
            }
        }
        sql "CREATE DATABASE IF NOT EXISTS ${db}"
        sql "DROP TABLE IF EXISTS ${db}.test_stream_load_2pc_owner"
        sql """CREATE TABLE ${db}.test_stream_load_2pc_owner (id BIGINT, username VARCHAR(20))
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")"""
        [owner, other].each { user ->
            sql "GRANT LOAD_PRIV ON ${db}.test_stream_load_2pc_owner TO ${user}"
        }

        [false, true].each { sqlHttp ->
            [false, true].each { byLabel ->
                ["commit", "abort"].each { operation ->
                    String label = "owner_${sqlHttp}_${byLabel}_${operation}_${UUID.randomUUID().toString()}"
                    def txn = prepare(sqlHttp, label)
                    def denied = finish(other, txn, byLabel, operation)
                    assertTrue(denied.status != "Success", denied.toString())
                    assertTrue(denied.toString().contains("not the owner"), denied.toString())
                    // A rejected request must leave the transaction available for its creator.
                    def completed = finish(owner, txn, byLabel, operation)
                    assertEquals("Success", completed.status)
                }
            }
        }
    } finally {
        try_sql "DROP USER ${owner}"
        try_sql "DROP USER ${other}"
    }
}
