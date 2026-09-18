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

suite("test_dml_stream_load_be_auth_off_docker", "docker,auth_call") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.feNum = 1
    options.beNum = 1

    docker(options) {
        String suiteName = "test_dml_stream_load_be_auth_off_docker"
        String tableUser = "${suiteName}_table_user"
        String dbUser = "${suiteName}_db_user"
        String pwd = "C123_567p"
        String dbName = "${suiteName}_db"
        String tableName = "${suiteName}_tb"
        String otherTableName = "${suiteName}_other_tb"
        String dataFile = "${context.file.parent}/../../data/auth_call/stream_load_data.csv"

        def fe = cluster.getFeByIndex(1)
        def be = cluster.getAllBackends(true).get(0)
        String feHttpAddress = "${fe.host}:${fe.httpPort}"
        String beHttpAddress = "${be.host}:${be.httpPort}"

        def parseCurlResult = { String out, String err ->
            def lines = out.readLines()
            assertTrue("curl output should include http code, output=${out}, err=${err}", !lines.isEmpty())
            def httpCode = lines.last() as int
            def body = lines.size() == 1 ? "" : lines[0..-2].join("\n")
            return [httpCode, body, err]
        }

        def streamLoad = { String authUser, String endpoint, String table, String label,
                           boolean followRedirect, List extraHeaders ->
            def command = [
                    "curl", "--noproxy", "*", "-sS", "-w", "\n%{http_code}",
                    "-u", "${authUser}:${pwd}",
                    "-H", "label:${label}",
                    "-H", "column_separator:,",
            ]
            if (followRedirect) {
                command.add("--location-trusted")
            }
            extraHeaders.each { command.addAll(["-H", it]) }
            command.addAll([
                    "-T", dataFile,
                    "http://${endpoint}/api/${dbName}/${table}/_stream_load"
            ])
            logger.info("stream load target: ${endpoint}, table: ${table}, label: ${label}, user: ${authUser}")
            def process = command.execute()
            process.waitForOrKill(7200000)
            def out = process.text.trim()
            def err = process.errorStream.text.trim()
            logger.info("stream load out: ${out}, err: ${err}")
            return parseCurlResult(out, err)
        }

        def streamLoad2pc = { String authUser, String table, def txnId, String txnOperation ->
            def command = [
                    "curl", "--noproxy", "*", "-sS", "-w", "\n%{http_code}",
                    "-X", "PUT",
                    "-u", "${authUser}:${pwd}",
                    "-H", "txn_id:${txnId}",
                    "-H", "txn_operation:${txnOperation}",
                    "http://${beHttpAddress}/api/${dbName}/${table}/_stream_load_2pc"
            ]
            logger.info("stream load 2pc target: ${beHttpAddress}, table: ${table}, "
                    + "txn operation: ${txnOperation}, user: ${authUser}")
            def process = command.execute()
            process.waitForOrKill(7200000)
            def out = process.text.trim()
            def err = process.errorStream.text.trim()
            logger.info("stream load 2pc out: ${out}, err: ${err}")
            return parseCurlResult(out, err)
        }

        def assertLoadFailed = { def result ->
            assertEquals(200, result[0])
            def json = parseJson(result[1])
            assertTrue("stream load should fail, body=${result[1]}", json.Status != "Success")
        }

        def assertLoadSuccess = { def result ->
            assertEquals(200, result[0])
            def json = parseJson(result[1])
            assertEquals("Success", json.Status)
            assertEquals(3, json.NumberTotalRows)
            assertEquals(3, json.NumberLoadedRows)
            return json
        }

        try {
            try_sql("DROP USER ${tableUser}")
            try_sql("DROP USER ${dbUser}")
            sql """DROP DATABASE IF EXISTS ${dbName}"""
            sql """CREATE USER '${tableUser}' IDENTIFIED BY '${pwd}'"""
            sql """CREATE USER '${dbUser}' IDENTIFIED BY '${pwd}'"""
            def clusters = sql """SHOW CLUSTERS"""
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${tableUser}"""
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${dbUser}"""

            sql """CREATE DATABASE ${dbName}"""
            sql """
                CREATE TABLE ${dbName}.${tableName} (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            sql """
                CREATE TABLE ${dbName}.${otherTableName} (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """

            assertLoadFailed(streamLoad(tableUser, beHttpAddress, tableName, "off_denied_before_grant",
                    false, []))

            sql """GRANT LOAD_PRIV ON ${dbName}.${tableName} TO ${tableUser}"""

            assertLoadSuccess(streamLoad(tableUser, beHttpAddress, tableName, "off_table_grant_be",
                    false, []))
            assertLoadSuccess(streamLoad(tableUser, feHttpAddress, tableName, "off_table_grant_fe",
                    true, []))
            def prepared = assertLoadSuccess(streamLoad(tableUser, beHttpAddress, tableName,
                    "off_table_grant_2pc", false, ["two_phase_commit:true"]))
            def committed = streamLoad2pc(tableUser, tableName, prepared.TxnId, "commit")
            assertEquals(200, committed[0])
            assertEquals("Success", parseJson(committed[1]).status)

            assertLoadFailed(streamLoad(tableUser, beHttpAddress, otherTableName,
                    "off_denied_other_table", false, []))

            sql """GRANT LOAD_PRIV ON ${dbName}.* TO ${dbUser}"""
            assertLoadSuccess(streamLoad(dbUser, beHttpAddress, otherTableName, "off_db_grant_be",
                    false, []))

            def tableCount = sql """SELECT COUNT(*) FROM ${dbName}.${tableName}"""
            assertEquals(9, tableCount[0][0] as int)
            def otherTableCount = sql """SELECT COUNT(*) FROM ${dbName}.${otherTableName}"""
            assertEquals(3, otherTableCount[0][0] as int)
        } finally {
            try_sql """DROP DATABASE IF EXISTS ${dbName}"""
            try_sql("DROP USER ${tableUser}")
            try_sql("DROP USER ${dbUser}")
        }
    }
}

suite("test_dml_stream_load_be_auth_on_docker", "docker,auth_call") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.feNum = 1
    options.beNum = 1
    options.beConfigs.add("enable_all_http_auth=true")

    docker(options) {
        String suiteName = "test_dml_stream_load_be_auth_on_docker"
        String tableUser = "${suiteName}_table_user"
        String dbUser = "${suiteName}_db_user"
        String pwd = "C123_567p"
        String dbName = "${suiteName}_db"
        String tableName = "${suiteName}_tb"
        String otherTableName = "${suiteName}_other_tb"
        String dataFile = "${context.file.parent}/../../data/auth_call/stream_load_data.csv"

        def fe = cluster.getFeByIndex(1)
        def be = cluster.getAllBackends(true).get(0)
        String feHttpAddress = "${fe.host}:${fe.httpPort}"
        String beHttpAddress = "${be.host}:${be.httpPort}"

        def parseCurlResult = { String out, String err ->
            def lines = out.readLines()
            assertTrue("curl output should include http code, output=${out}, err=${err}", !lines.isEmpty())
            def httpCode = lines.last() as int
            def body = lines.size() == 1 ? "" : lines[0..-2].join("\n")
            return [httpCode, body, err]
        }

        def streamLoad = { String authUser, String endpoint, String table, String label,
                           boolean followRedirect, List extraHeaders ->
            def command = [
                    "curl", "--noproxy", "*", "-sS", "-w", "\n%{http_code}",
                    "-u", "${authUser}:${pwd}",
                    "-H", "label:${label}",
                    "-H", "column_separator:,",
            ]
            if (followRedirect) {
                command.add("--location-trusted")
            }
            extraHeaders.each { command.addAll(["-H", it]) }
            command.addAll([
                    "-T", dataFile,
                    "http://${endpoint}/api/${dbName}/${table}/_stream_load"
            ])
            logger.info("stream load target: ${endpoint}, table: ${table}, label: ${label}, user: ${authUser}")
            def process = command.execute()
            process.waitForOrKill(7200000)
            def out = process.text.trim()
            def err = process.errorStream.text.trim()
            logger.info("stream load out: ${out}, err: ${err}")
            return parseCurlResult(out, err)
        }

        def streamLoad2pc = { String authUser, String table, def txnId, String txnOperation ->
            def command = [
                    "curl", "--noproxy", "*", "-sS", "-w", "\n%{http_code}",
                    "-X", "PUT",
                    "-u", "${authUser}:${pwd}",
                    "-H", "txn_id:${txnId}",
                    "-H", "txn_operation:${txnOperation}",
                    "http://${beHttpAddress}/api/${dbName}/${table}/_stream_load_2pc"
            ]
            logger.info("stream load 2pc target: ${beHttpAddress}, table: ${table}, "
                    + "txn operation: ${txnOperation}, user: ${authUser}")
            def process = command.execute()
            process.waitForOrKill(7200000)
            def out = process.text.trim()
            def err = process.errorStream.text.trim()
            logger.info("stream load 2pc out: ${out}, err: ${err}")
            return parseCurlResult(out, err)
        }

        def assertLoadFailed = { def result ->
            assertEquals(200, result[0])
            def json = parseJson(result[1])
            assertTrue("stream load should fail, body=${result[1]}", json.Status != "Success")
        }

        def assertLoadSuccess = { def result ->
            assertEquals(200, result[0])
            def json = parseJson(result[1])
            assertEquals("Success", json.Status)
            assertEquals(3, json.NumberTotalRows)
            assertEquals(3, json.NumberLoadedRows)
            return json
        }

        try {
            try_sql("DROP USER ${tableUser}")
            try_sql("DROP USER ${dbUser}")
            sql """DROP DATABASE IF EXISTS ${dbName}"""
            sql """CREATE USER '${tableUser}' IDENTIFIED BY '${pwd}'"""
            sql """CREATE USER '${dbUser}' IDENTIFIED BY '${pwd}'"""
            def clusters = sql """SHOW CLUSTERS"""
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${tableUser}"""
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${dbUser}"""

            sql """CREATE DATABASE ${dbName}"""
            sql """
                CREATE TABLE ${dbName}.${tableName} (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            sql """
                CREATE TABLE ${dbName}.${otherTableName} (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """

            assertLoadFailed(streamLoad(tableUser, beHttpAddress, tableName, "on_denied_before_grant",
                    false, []))

            sql """GRANT LOAD_PRIV ON ${dbName}.${tableName} TO ${tableUser}"""

            assertLoadSuccess(streamLoad(tableUser, beHttpAddress, tableName, "on_table_grant_be",
                    false, []))
            assertLoadSuccess(streamLoad(tableUser, feHttpAddress, tableName, "on_table_grant_fe",
                    true, []))
            def prepared = assertLoadSuccess(streamLoad(tableUser, beHttpAddress, tableName,
                    "on_table_grant_2pc", false, ["two_phase_commit:true"]))
            def committed = streamLoad2pc(tableUser, tableName, prepared.TxnId, "commit")
            assertEquals(200, committed[0])
            assertEquals("Success", parseJson(committed[1]).status)

            assertLoadFailed(streamLoad(tableUser, beHttpAddress, otherTableName,
                    "on_denied_other_table", false, []))

            sql """GRANT LOAD_PRIV ON ${dbName}.* TO ${dbUser}"""
            assertLoadSuccess(streamLoad(dbUser, beHttpAddress, otherTableName, "on_db_grant_be",
                    false, []))

            def tableCount = sql """SELECT COUNT(*) FROM ${dbName}.${tableName}"""
            assertEquals(9, tableCount[0][0] as int)
            def otherTableCount = sql """SELECT COUNT(*) FROM ${dbName}.${otherTableName}"""
            assertEquals(3, otherTableCount[0][0] as int)
        } finally {
            try_sql """DROP DATABASE IF EXISTS ${dbName}"""
            try_sql("DROP USER ${tableUser}")
            try_sql("DROP USER ${dbUser}")
        }
    }
}
