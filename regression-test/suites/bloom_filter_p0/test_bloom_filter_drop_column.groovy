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
suite("test_bloom_filter_drop_column") {
    def table_name = "test_bloom_filter_drop_column"

    sql """drop TABLE if exists ${table_name}"""

    sql """CREATE TABLE IF NOT EXISTS ${table_name} (
      `a` varchar(150) NULL,
      `c1` varchar(10), 
      `c2` varchar(10)
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`)
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "bloom_filter_columns" = "c1, c2",
    "in_memory" = "false",
    "storage_format" = "V2"
    )"""
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { tableName, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def assertShowCreateTableWithRetry = { tableName, expectedCondition, contains, maxRetries, waitSeconds ->
        int attempt = 0
        while (attempt < maxRetries) {
            def res = sql """SHOW CREATE TABLE ${tableName}"""
            log.info("Attempt ${attempt + 1}: show table: ${res}")
            if (res && res.size() > 0 && ((contains && res[0][1].contains(expectedCondition)) || (!contains && !res[0][1].contains(expectedCondition)))) {
                logger.info("Attempt ${attempt + 1}: Condition met.")
                return
            } else {
                logger.warn("Attempt ${attempt + 1}: Condition not met. Retrying after ${waitSeconds} second(s)...")
            }
            attempt++
            if (attempt < maxRetries) {
                sleep(waitSeconds * 1000)
            }
        }
        def finalRes = sql """SHOW CREATE TABLE ${tableName}"""
        log.info("Final attempt: show table: ${finalRes}")
        assertTrue(finalRes && finalRes.size() > 0, "SHOW CREATE TABLE return empty or null")
        if (contains) {
            assertTrue(finalRes[0][1].contains(expectedCondition), "expected to contain \"${expectedCondition}\", actual: ${finalRes[0][1]}")
        } else {
            assertTrue(!finalRes[0][1].contains(expectedCondition), "expected not to contain \"${expectedCondition}\", actual: ${finalRes[0][1]}")
        }
    }

    sql """INSERT INTO ${table_name} values ('1', '1', '1')"""
    sql "sync"

    qt_select """select * from ${table_name} order by a"""

    assertShowCreateTableWithRetry(table_name, "\"bloom_filter_columns\" = \"c1, c2\"", true, 3, 30)
    // drop column c1
    sql """ALTER TABLE ${table_name} DROP COLUMN c1"""
    wait_for_latest_op_on_table_finish(table_name, timeout)
    sql "sync"

    // show create table with retry logic
    assertShowCreateTableWithRetry(table_name, "\"bloom_filter_columns\" = \"c2\"", true, 3, 30)

    // drop column c2
    sql """ALTER TABLE ${table_name} DROP COLUMN c2"""
    wait_for_latest_op_on_table_finish(table_name, timeout)
    sql "sync"

    // show create table with retry logic
    assertShowCreateTableWithRetry(table_name, "\"bloom_filter_columns\" = \"\"", false, 3, 30)

    // add new column c1
    sql """ALTER TABLE ${table_name} ADD COLUMN c1 ARRAY<STRING>"""
    wait_for_latest_op_on_table_finish(table_name, timeout)
    sql "sync"

    // insert data
    sql """INSERT INTO ${table_name} values ('2', null)"""
    sql "sync"
    // select data
    qt_select """select * from ${table_name} order by a"""
}
