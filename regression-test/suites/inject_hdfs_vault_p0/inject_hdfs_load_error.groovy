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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("inject_hdfs_load_error", "nonConcurrent") {
    if (!enableStoragevault()) {
        logger.info("skip create storgage vault case")
        return
    }

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS inject_hdfs_load_error
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${getHmsHdfsFs()}",
        "path_prefix" = "inject_hdfs_load_error"
        );
    """

    try {
        String backendId;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backendId = backendId_to_backendIP.keySet()[0]
        def be_host = backendId_to_backendIP.get(backendId)
        def be_http_port = backendId_to_backendHttpPort.get(backendId)

        def tableName = "inject_load_error_table"

        // op = {set, apply_suite, clear}, behavior = {sleep, return, return_ok, return_error}
        // name = {point_name}, code = {return_error return code}, duration = {sleep duration}
        // value is code for return_error, duration for sleep
        // internal error is 6
        def triggerInject = { name, op, behavior, value ->
            // trigger compactions for all tablets in ${tableName}
            StringBuilder sb = new StringBuilder();
            // /api/injection_point/{op}/{name}
            sb.append("curl -X POST http://${be_host}:${be_http_port}")
            sb.append("/api/injection_point/${op}/${name}/${behavior}/${value}")

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("enable inject, op ${op}, name ${name}, behavior ${behavior}, value ${value}")
            assertEquals(code, 0)
            return out
        }

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        INTEGER NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1", "storage_vault_name" = "inject_hdfs_load_error"
            )
        """

        def load_table = { cnt ->
            for (int i = 0; i < cnt; i++) {
                sql """
                    insert into ${tableName} values(${i}, '${i}');
                """
            }
        }

        for (int j = 1; j < 5; j++) {
            triggerInject("HdfsFileWriter::hdfsFlush", "set", "return_error", 6)
            expectExceptionLike({
                load_table(j)
            }, "failed to flush hdfs file")
            triggerInject("HdfsFileWriter::hdfsFlush", "clear", "valid", 0)
        }

        for (int j = 1; j < 5; j++) {
            triggerInject("HdfsFileWriter::hdfsCloseFile", "set", "return_error", 6)
            expectExceptionLike({
                load_table(j)
            }, "Write hdfs file failed")
            triggerInject("HdfsFileWriter::hdfsCloseFile", "clear",  "valid", 0)
        }

        for (int j = 1; j < 5; j++) {
            triggerInject("HdfsFileWriter::append_hdfs_file_error", "set", "return_error", 6)
            expectExceptionLike({
                load_table(j)
            }, "write hdfs file failed")
            triggerInject("HdfsFileWriter::append_hdfs_file_error", "clear",  "valid", 0)
        }

    } finally {
    }
}