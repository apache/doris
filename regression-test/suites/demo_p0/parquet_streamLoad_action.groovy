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

suite("parquet_streamLoad_action") {

    def tableName = "test_parquet_streamload_action"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
            CREATE TABLE IF NOT EXISTS ${tableName}
            (
                id        VARCHAR(255) NOT NULL,
                name        VARCHAR(255) NOT NULL,
                password      VARCHAR(255) NOT NULL
            )
            DUPLICATE KEY(id, name, password)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """


    def backendIps = [:]
    def backendHttpPorts = [:]
    getBackendIpHttpPort(backendIps, backendHttpPorts)
    def backendId = backendIps.keySet()[0]
    streamLoad {
        table tableName
        set 'format', 'parquet'
        set 'Expect', '100-continue'
        file 'streamload.parquet'

        // can direct to backend, then this backend is the txn coordinator.
        directToBe  backendIps.get(backendId),  backendHttpPorts.get(backendId) as int

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals("[NOT_IMPLEMENTED_ERROR]stream load not support parquet format, please use broker load", json.Message)
        }
    }


    streamLoad {
        table tableName
        set 'format', 'parquet'
        set 'Expect', '100-continue'
        file 'streamload.parquet'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals("[NOT_IMPLEMENTED_ERROR]stream load not support parquet format, please use broker load", json.Message)
        }
    }

}
