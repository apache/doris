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
      `c1` varchar(10)
    ) ENGINE=OLAP
    DUPLICATE KEY(`a`)
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "bloom_filter_columns" = "c1",
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
    sql """INSERT INTO ${table_name} values ('1', '1')"""

    qt_select """select * from ${table_name} order by a"""

    // drop column c1
    sql """ALTER TABLE ${table_name} DROP COLUMN c1"""
    wait_for_latest_op_on_table_finish(table_name, timeout)

    // show create table
    def res = sql """SHOW CREATE TABLE ${table_name}"""
    log.info("show table:{}", res);
    assert res[0][1].contains("\"bloom_filter_columns\" = \"\"")

    // add new column c1
    sql """ALTER TABLE ${table_name} ADD COLUMN c1 ARRAY<STRING>"""
    wait_for_latest_op_on_table_finish(table_name, timeout)

    // insert data
    sql """INSERT INTO ${table_name} values ('2', null)"""
    // select data
    qt_select """select * from ${table_name} order by a"""
}
