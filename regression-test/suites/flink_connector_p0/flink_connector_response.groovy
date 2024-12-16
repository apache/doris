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
// under the License./

import java.util.regex.Pattern

/**
 * Flink connector depends on these responses
 */
suite("flink_connector_response") {

    def LABEL_EXIST_PATTERN =
            Pattern.compile("Label \\[(.*)\\] has already been used, relate to txn \\[(\\d+)\\]");
    def COMMITTED_PATTERN =
            Pattern.compile(
                    "transaction \\[(\\d+)\\] is already \\b(COMMITTED|committed|VISIBLE|visible)\\b, not pre-committed.");
    def ABORTTED_PATTERN =
            Pattern.compile(
                    "transaction \\[(\\d+)\\] is already|transaction \\[(\\d+)\\] not found");

    def thisDb = sql """select database()""";
    thisDb = thisDb[0][0];
    logger.info("current database is ${thisDb}");

    def tableName = "test_response"
    sql """DROP TABLE IF EXISTS ${tableName}"""

    sql """
        CREATE TABLE `${tableName}` (
            `id` int,
            `name` string
        )
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true"
);
    """;

    def execute_stream_load_cmd = {label ->
        def filePath = "${context.config.dataPath}/flink_connector_p0/test_response.csv"
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl --location-trusted -u """ + context.config.feHttpUser + ":" + context.config.feHttpPassword)
        strBuilder.append(""" -H two_phase_commit:true """)
        strBuilder.append(""" -H column_separator:, """)
        strBuilder.append(""" -H expect:100-continue """)
        strBuilder.append(""" -H label:""" + label)
        strBuilder.append(""" -T """ + filePath)
        strBuilder.append(""" http://""" + context.config.feHttpAddress + """/api/${thisDb}/${tableName}/_stream_load""")

        String command = strBuilder.toString()
        logger.info("streamload command=" + command)
        def process = command.toString().execute()
        process.waitFor()
        def out = process.getText()
        println(out)
        out
    }

    def execute_commit_cmd = {txnId ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -X PUT --location-trusted -u """ + context.config.feHttpUser + ":" + context.config.feHttpPassword)
        strBuilder.append(""" -H txn_id:""" + txnId)
        strBuilder.append(""" -H txn_operation:commit""")
        strBuilder.append(""" http://""" + context.config.feHttpAddress + """/api/${thisDb}/${tableName}/_stream_load_2pc""")

        String command = strBuilder.toString()
        logger.info("streamload command=" + command)
        def processCommit = command.toString().execute()
        processCommit.waitFor()
        def outCommit = processCommit.getText()
        println(outCommit)
        outCommit
    }

    def execute_abort_cmd = {txnId ->
        StringBuilder strBuilder = new StringBuilder()
        strBuilder.append("""curl -X PUT --location-trusted -u """ + context.config.feHttpUser + ":" + context.config.feHttpPassword)
        strBuilder.append(""" -H txn_id:""" + txnId)
        strBuilder.append(""" -H txn_operation:abort""")
        strBuilder.append(""" http://""" + context.config.feHttpAddress + """/api/${thisDb}/${tableName}/_stream_load_2pc""")

        String command = strBuilder.toString()
        logger.info("streamload command=" + command)
        def processAbort = command.toString().execute()
        processAbort.waitFor()
        def outAbort = processAbort.getText()
        println(outAbort)
        outAbort
    }

    //normal stream load
    def uuid = UUID.randomUUID().toString().replaceAll("-", "");
    String out = execute_stream_load_cmd(uuid)
    def jsonout = parseJson(out);
    assertEquals(jsonout.Status, "Success")
    assertEquals(jsonout.Label, uuid)
    assertEquals(jsonout.NumberTotalRows, 2)
    def txnid = jsonout.TxnId

    //label exist load
    println("start label exists load")
    def outLabelExist = execute_stream_load_cmd(uuid)
    def outLabelExistJson = parseJson(outLabelExist);
    assertEquals(outLabelExistJson.Status, "Label Already Exists")
    assertTrue(LABEL_EXIST_PATTERN.matcher(outLabelExistJson.Message).find());

    def no_exist_txnid = Long.MAX_VALUE;
    //abort txnid
    println("start abort txnid")
    def outAbort = execute_abort_cmd(txnid)
    jsonout = parseJson(outAbort);
    assertEquals(jsonout.status, "Success")

    //abort not exist txnid
    println("start abort not exist txnid")
    outAbort = execute_abort_cmd(no_exist_txnid)
    jsonout = parseJson(outAbort)
    assertNotEquals(jsonout.status, "Success")
    assertTrue(ABORTTED_PATTERN.matcher(jsonout.msg).find())

    //abort already abort txnid
    println("start abort already abort txnid")
    outAbort = execute_abort_cmd(txnid);
    jsonout = parseJson(outAbort)
    assertNotEquals(jsonout.status, "Success")
    assertTrue(ABORTTED_PATTERN.matcher(jsonout.msg).find())

    //commit not exist txnid
    println("start commit not exist txnid")
    def outCommit = execute_commit_cmd(no_exist_txnid)
    jsonout = parseJson(outAbort)
    assertNotEquals(jsonout.status, "Success")
    assertTrue(ABORTTED_PATTERN.matcher(jsonout.msg).find())

    //commit already abort txnid
    println("start commit already abort txnid")
    outCommit = execute_commit_cmd(txnid)
    jsonout = parseJson(outCommit)
    assertNotEquals(jsonout.status, "Success")
    assertTrue(ABORTTED_PATTERN.matcher(jsonout.msg).find())


    //start new normal stream load
    println("start new stream load")
    uuid = UUID.randomUUID().toString().replaceAll("-", "");
    out = execute_stream_load_cmd(uuid)
    jsonout = parseJson(out);
    assertEquals(jsonout.Status, "Success")
    assertEquals(jsonout.Label, uuid)
    assertEquals(jsonout.NumberTotalRows, 2)
    //commit txnid
    def commitTxnId = jsonout.TxnId
    out = execute_commit_cmd(commitTxnId)
    jsonout = parseJson(out);
    assertEquals(jsonout.status, "Success")

    //commit already commit txnid
    println("start commit already commit txnid")
    outCommit = execute_commit_cmd(commitTxnId)
    jsonout = parseJson(outCommit)
    assertNotEquals(jsonout.status, "Success")
    assertTrue(COMMITTED_PATTERN.matcher(jsonout.msg).find())

    //abort already commit txnid
    println("start abort already commit txnid")
    outAbort = execute_abort_cmd(commitTxnId)
    jsonout = parseJson(outAbort)
    assertNotEquals(jsonout.status, "Success")
    assertTrue(ABORTTED_PATTERN.matcher(jsonout.msg).find())
}

