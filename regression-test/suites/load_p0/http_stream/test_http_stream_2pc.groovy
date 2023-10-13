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

suite("test_http_stream_2pc", "p0") {

   
    // 1. test two phase commit
    def tableName1 = "test_http_stream_2pc"
    def db = "regression_test_load_p0_http_stream"
    try {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
            id int,
            name CHAR(10),
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP,
            dt_2 DATETIMEV2 DEFAULT CURRENT_TIMESTAMP,
            dt_3 DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP,
            dt_4 DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        )
        """

        def json 
        streamLoad {
            set 'version', '1'
            set 'two_phase_commit', 'true'
            set 'sql', """
                    insert into ${db}.${tableName1} (id, name) select c1, c2 from http_stream("format"="csv")
                    """
            time 10000
            file 'test_http_stream.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("http_stream result: ${result}".toString())
                json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals("true", json.TwoPhaseCommit.toLowerCase())
                assertEquals(11, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword} -H txn_id:${json.TxnId} -H txn_operation:commit http://${context.config.feHttpAddress}/api/${db}/${tableName1}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        code = process.waitFor()
        out = process.text
        json2pc = parseJson(out)
        log.info("http_stream 2pc result: ${out}".toString())
        assertEquals(code, 0)
        assertEquals("success", json2pc.status.toLowerCase())

        qt_sql "select id, name from ${tableName1} order by id"
    } finally {
        try_sql "DROP TABLE IF EXISTS ${tableName1}"
    }
}

