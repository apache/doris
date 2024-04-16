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

suite("inject_hdfs_load_error") {
    if (!enableStoragevault()) {
        logger.info("skip create storgage vault case")
        return
    }

    sql """
        CREATE STORAGE VAULT IF NOT EXISTS inject_hdfs_select_error
        PROPERTIES (
        "type"="hdfs",
        "fs.defaultFS"="${getHdfsFs()}",
        "path_prefix" = "inject_hdfs_select_error"
        );
    """

    try {
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def be_host = backendId_to_backendIP.get(backendId)
        def be_http_port = backendId_to_backendHttpPort.get(backendId)

        def tableName = "inject_select_error_table"
        
        def triggerInject = { op, name, behavior ->
            // trigger compactions for all tablets in ${tableName}
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://${be_host}:${be_http_port}")
            sb.append("/api/injection_point/{$op}/{$name}/{$behavior}")

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("enable inject, op ${op}, name ${name}, behavior {$behavior}")
            assertEquals(code, 0)
            return out
        }

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1", "storage_vault_name" = "inject_hdfs_select_error"
            )
        """

        def load_table = { cnt ->
            for (int i = 0; i < cnt; i++) {
                sql """
                    insert into ${tableName} values(i, '${i}');
                """
            }
        }

        for (int j = 1; j <10; j++) {
            load_table(j)
        }

        for (int j = 1; j < 5; j++) {
            triggerInject("HdfsFileReader:read_error", "set", "return_error")
            expectExceptionLike({
                load_table(j)
            }, "Read hdfs file failed")
            triggerInject("HdfsFileReader:read_error", "clear")
        }

    } finally {
    }
}