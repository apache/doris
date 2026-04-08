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

import org.junit.Assert;
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_dml_stream_load_auth","p0,auth_call") {
    String user = 'test_dml_stream_load_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dml_stream_load_auth_db'
    String tableName = 'test_dml_stream_load_auth_tb'

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""
    sql """create table ${dbName}.${tableName} (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""

    def write_to_file = { cur_path, content ->
        File file = new File(cur_path)
        file.write(content)
    }

    def jdbcUrl = context.config.jdbcUrl
    def urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    String feHttpAddress = context.config.feHttpAddress
    def http_port = feHttpAddress.substring(feHttpAddress.indexOf(":") + 1)

    def path_file = "${context.file.parent}/../../data/auth_call/stream_load_data.csv"
    def load_path = "${context.file.parent}/../../data/auth_call/stream_load_cm.sh"
    def tlsInfo = null
    def protocol = "http"
    if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
        tlsInfo = " --cert " + context.config.otherConfigs.get("trustCert") + " --cacert " + context.config.otherConfigs.get("trustCACert") + " --key " + context.config.otherConfigs.get("trustCAKey")
        protocol = "https"
    }
    def cm = """curl --location-trusted -u ${user}:${pwd} -H "column_separator:," -T ${path_file} ${protocol}://${sql_ip}:${http_port}/api/${dbName}/${tableName}/_stream_load ${tlsInfo}"""
    logger.info("cm: " + cm)
    write_to_file(load_path, cm)
    cm = "bash " + load_path
    logger.info("cm:" + cm)

    // Added a retry mechanism as the service instability may cause intermittent failures
    def maxRetries = 3
    def retryCount = 0
    def success = false
    def sout = ""
    def serr = ""

    while (retryCount < maxRetries && !success) {
        retryCount++
        def proc = cm.execute()
        proc.waitForOrKill(7200000)
        sout = proc.inputStream.text.trim()
        serr = proc.errorStream.text.trim()
        logger.info("std out: " + sout)
        logger.info("std err: " + serr)

        if (sout.contains("denied")) {
            success = true
            logger.info("Success: 'denied' message received.")
        } else {
            if (retryCount < maxRetries) {
                logger.warn("Attempt ${retryCount} failed. Retrying in 2 seconds...")
                sleep(2000)
            }
        }
    }

    // Final assertion: If all three retries fail, throw an error.
    assertTrue(success, "After ${maxRetries} attempts, expected 'denied' message not found. \nFinal StdOut: ${sout} \nFinal StdErr: ${serr}")

    sql """grant load_priv on ${dbName}.${tableName} to ${user}"""

    maxRetries = 3
    retryCount = 0
    success = false
    def sout2 = ""
    def serr2 = ""

    while (retryCount < maxRetries && !success) {
        retryCount++
        def proc = cm.execute()
        proc.waitForOrKill(7200000)
        sout2 = proc.inputStream.text.trim()
        serr2 = proc.errorStream.text.trim()
        logger.info("std out: " + sout2)
        logger.info("std err: " + serr2)

        if (sout2.indexOf("denied") == -1) {
            success = true
        } else {
            if (retryCount < maxRetries) {
                logger.warn("Attempt ${retryCount} still showing 'denied'. Retrying after 2s...")
                sleep(2000)
            }
        }
    }

    assertTrue(success, "Unexpected 'denied' message in response after ${maxRetries} attempts: '${sout2}'")

    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql """SHOW TRANSACTION FROM ${dbName} WHERE ID=111;"""
            exception "denied"
        }
    }

    sql """grant admin_priv on *.*.* to ${user}"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql """SHOW TRANSACTION FROM ${dbName} WHERE ID=111;"""
            exception "exist"
        }
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
