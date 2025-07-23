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

suite("test_partial_update_publish_all_del") {
    if (isCloudMode()) {
        logger.info("skip test_partial_update_publish_all_del in cloud mode")
        return
    }
    def dbName = context.config.getDbNameByFile(context.file)

    def tableName = "test_partial_update_publish_all_del"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NULL COMMENT "用户姓名",
                `score` int(11) NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """

    sql """insert into ${tableName} values
        (2, "doris2", 2000, 223, 2),
        (1, "doris", 1000, 123, 1),
        (5, "doris5", 5000, 523, 5),
        (4, "doris4", 4000, 423, 4),
        (3, "doris3", 3000, 323, 3);"""
    qt_sql "select * from ${tableName} order by id;"

    def do_streamload_2pc_commit = { txnId ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${dbName}/${tableName}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        def code = process.waitFor()
        def out = process.text
        def json2pc = parseJson(out)
        log.info("http_stream 2pc result: ${out}".toString())
        assertEquals(code, 0)
        assertEquals("success", json2pc.status.toLowerCase())
    }

    def wait_for_publish = {txnId, waitSecond ->
        String st = "PREPARE"
        while (!st.equalsIgnoreCase("VISIBLE") && !st.equalsIgnoreCase("ABORTED") && waitSecond > 0) {
            Thread.sleep(1000)
            waitSecond -= 1
            def result = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
            assertNotNull(result)
            st = result[0].TransactionStatus
        }
        log.info("Stream load with txn ${txnId} is ${st}")
        assertEquals(st, "VISIBLE")
    }

    def txnId1, txnId2, txnId3

    String data1 = """1,"ddddddddddd"\n2,"eeeeee"\n3,"aaaaa"\n4,"bbbbbbbb"\n5,"cccccccccccc"\n"""
    streamLoad {
        table "${tableName}"
        set 'two_phase_commit', 'true'
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,name'

        inputStream new ByteArrayInputStream(data1.getBytes())
        time 10000 // limit inflight 10s
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            txnId1 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        } 

    }

    String data2 = """1,10\n2,20\n3,30\n4,40\n5,50\n"""
    streamLoad {
        table "${tableName}"
        set 'two_phase_commit', 'true'
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,dft'

        inputStream new ByteArrayInputStream(data2.getBytes())
        time 10000 // limit inflight 10s
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            txnId2 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        } 
    }

    // all rows have delete sign marks
    String data3 = """1,1\n3,1\n5,1\n"""
    streamLoad {
        table "${tableName}"
        set 'two_phase_commit', 'true'
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,__DORIS_DELETE_SIGN__'

        inputStream new ByteArrayInputStream(data3.getBytes())
        time 10000 // limit inflight 10s
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            txnId3 = json.TxnId
            assertEquals("success", json.Status.toLowerCase())
        } 
    }

    do_streamload_2pc_commit(txnId1)
    wait_for_publish(txnId1, 60)
    do_streamload_2pc_commit(txnId2)
    wait_for_publish(txnId2, 60)
    do_streamload_2pc_commit(txnId3)
    wait_for_publish(txnId3, 60)

    qt_sql "select * from ${tableName} order by id;"
}