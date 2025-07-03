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

suite("test_index_builder_drop_index_fault_injection", "nonConcurrent") {
    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
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
    def runTest = { indexTbName ->
        sql """ insert into ${indexTbName} values(1, "json love anny", "json", "anny",1); """
        sql "sync"

        def show_result = sql_return_maparray "show index from ${indexTbName}"
        logger.info("show index from " + indexTbName + " result: " + show_result)
        assertEquals(show_result.size(), 4)
        assertEquals(show_result[0].Key_name, "index_int")
        assertEquals(show_result[1].Key_name, "index_str_k2")
        assertEquals(show_result[2].Key_name, "index_str_k4")
        assertEquals(show_result[3].Key_name, "index_k5")

        try {
            GetDebugPoint().enableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index", [indexes_count: 3])
            sql "DROP INDEX index_int ON ${indexTbName}"
            wait_for_latest_op_on_table_finish(indexTbName, timeout)
            show_result = sql_return_maparray "show index from ${indexTbName}"
            logger.info("show index from " + indexTbName + " result: " + show_result)
            assertEquals(show_result.size(), 3)
            assertEquals(show_result[0].Key_name, "index_str_k2")
            assertEquals(show_result[1].Key_name, "index_str_k4")
            assertEquals(show_result[2].Key_name, "index_k5")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index")
        }

        try {
            GetDebugPoint().enableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index", [indexes_count: 2])
            sql "DROP INDEX index_str_k2 ON ${indexTbName}"
            wait_for_latest_op_on_table_finish(indexTbName, timeout)
            show_result = sql_return_maparray "show index from ${indexTbName}"
            logger.info("show index from " + indexTbName + " result: " + show_result)
            assertEquals(show_result.size(), 2)
            assertEquals(show_result[0].Key_name, "index_str_k4")
            assertEquals(show_result[1].Key_name, "index_k5")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index")
        }

        try {
            GetDebugPoint().enableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index", [indexes_count: 1])
            sql "DROP INDEX index_str_k4 ON ${indexTbName}"
            wait_for_latest_op_on_table_finish(indexTbName, timeout)
            show_result = sql_return_maparray "show index from ${indexTbName}"
            logger.info("show index from " + indexTbName + " result: " + show_result)
            assertEquals(show_result.size(), 1)
            assertEquals(show_result[0].Key_name, "index_k5")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index")
        }

        try {
            GetDebugPoint().enableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index", [indexes_count: 0])
            sql "DROP INDEX index_k5 ON ${indexTbName}"
            wait_for_latest_op_on_table_finish(indexTbName, timeout)
            show_result = sql_return_maparray "show index from ${indexTbName}"
            logger.info("show index from " + indexTbName + " result: " + show_result)
            assertEquals(show_result.size(), 0)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("index_builder.update_inverted_index_info.drop_index")
        }
    }

    def createTestTable = { version ->
        def indexTbName = "test_index_builder_drop_index_fault_injection_${version}"

        sql "DROP TABLE IF EXISTS ${indexTbName}"
        sql """
          CREATE TABLE ${indexTbName}
          (
              k1 int ,
              k2 string,
              k3 char(50),
              k4 varchar(200),
              k5 int,
              index index_int (k1) using inverted,
              index index_str_k2 (k2) using inverted properties("parser"="english","ignore_above"="257"),
              index index_str_k4 (k4) using inverted,
              index index_k5 (k5) using inverted
          )
          DISTRIBUTED BY RANDOM BUCKETS 10
          PROPERTIES("replication_num" = "1","inverted_index_storage_format"="${version}");
        """

        runTest(indexTbName)
    }

    createTestTable("v1")
    createTestTable("v2")

}