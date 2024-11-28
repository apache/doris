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

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
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
    def cm = """curl -v --location-trusted -u ${user}:${pwd} -H "column_separator:," -T ${path_file} http://${sql_ip}:${http_port}/api/${dbName}/${tableName}/_stream_load"""
    logger.info("cm: " + cm)
    write_to_file(load_path, cm)
    cm = "bash " + load_path
    logger.info("cm:" + cm)


    def proc = cm.execute()
    def sout = new StringBuilder(), serr = new StringBuilder()
    proc.consumeProcessOutput(sout, serr)
    proc.waitForOrKill(7200000)
    logger.info("std out: " + sout + "std err: " + serr)
    assertTrue(sout.toString().indexOf("FAILED") != -1)
    assertTrue(sout.toString().indexOf("denied") != -1)


    sql """grant load_priv on ${dbName}.${tableName} to ${user}"""

    proc = cm.execute()
    sout = new StringBuilder()
    serr = new StringBuilder()
    proc.consumeProcessOutput(sout, serr)
    proc.waitForOrKill(7200000)
    logger.info("std out: " + sout + "std err: " + serr)
    assertTrue(sout.toString().indexOf("Success") != -1)

    int pos1 = sout.indexOf("TxnId")
    int pos2 = sout.indexOf(",", pos1)
    int pos3 = sout.indexOf(":", pos1)
    def tsc_id = sout.substring(pos3+2, pos2)

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """SHOW TRANSACTION FROM ${dbName} WHERE ID=${tsc_id};"""
            exception "denied"
        }
    }

    def res = sql """select count() from ${dbName}.${tableName}"""
    assertTrue(res[0][0] == 3)

    def stream_res = sql """SHOW STREAM LOAD FROM ${dbName};"""
    logger.info("stream_res: " + stream_res)

    sql """grant admin_priv on *.*.* to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        def transaction_res = sql """SHOW TRANSACTION FROM ${dbName} WHERE ID=${tsc_id};"""
        assertTrue(transaction_res.size() == 1)
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
