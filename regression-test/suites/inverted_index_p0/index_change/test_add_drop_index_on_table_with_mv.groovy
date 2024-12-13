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

suite("test_add_drop_index_on_table_with_mv") {
    def tableName = "test_add_drop_index_on_table_with_mv"

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            logger.info("expected_finished_num: " + expected_finished_num)
            // check only base table build index job
            assertEquals(1, expected_finished_num)
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }

    def getJobState = { table ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${table}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
          `load_time` datetime NOT NULL COMMENT '事件发生时间',
          `id` varchar(192) NOT NULL COMMENT '',
          `class` varchar(192) NOT NULL COMMENT '',
          `result` int NOT NULL COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`load_time`,`id`, `class`)
        COMMENT ''
        PARTITION BY RANGE(`load_time`)(
            PARTITION p1 VALUES LESS THAN ("2025-01-01 00:00:00")
        )
        DISTRIBUTED BY HASH(`load_time`,`id`, `class`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${tableName} values ('2024-03-20 10:00:00', 'a', 'b', 1) """

    sql """
        create materialized view mv_1 as
        select 
          date_trunc(load_time, 'minute'),
          id,
          class,
          count(id) as total,
          min(result) as min_result,
          sum(result) as max_result
        from 
          ${tableName}
        group by date_trunc(load_time, 'minute'), id, class;
    """

    sql """ SHOW ALTER TABLE MATERIALIZED VIEW """

    def max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tableName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    // create inverted index
    sql """ CREATE INDEX idx1 ON ${tableName}(class) USING INVERTED; """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    def show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 1)
    assertEquals(show_result[0][2], "idx1")

    // build index
    sql """ BUILD INDEX idx1 ON ${tableName}; """

    // wait for index build finish
    wait_for_build_index_on_partition_finish(tableName, timeout)
}
