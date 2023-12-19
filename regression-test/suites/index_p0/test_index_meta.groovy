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

import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("index_meta", "p0") {
    // prepare test table
    def timeout = 120000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(useTime = 0; useTime <= OpTimeout; useTime += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def tableName = "test_index_meta"

    sql "DROP TABLE IF EXISTS ${tableName}"
    // create 1 replica table
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `id` INT NULL,
                `name` STRING NULL,
                `description` STRING NULL,
                INDEX idx_id (`id`) USING BITMAP COMMENT 'index for id',
                INDEX idx_name (`name`) USING INVERTED PROPERTIES("parser"="none") COMMENT 'index for name'
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            properties("replication_num" = "1");
    """

    def var_result = sql "show variables"
    logger.info("show variales result: " + var_result )

    // show index of create table
    def show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 2)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[0][4], "id")
    assertEquals(show_result[0][10], "BITMAP")
    assertEquals(show_result[0][11], "'index for id'")
    assertEquals(show_result[0][12], "")
    assertEquals(show_result[1][2], "idx_name")
    assertEquals(show_result[1][4], "name")
    assertEquals(show_result[1][10], "INVERTED")
    assertEquals(show_result[1][11], "'index for name'")
    assertEquals(show_result[1][12], "(\"parser\" = \"none\")")

    // add index on column description
    sql "create index idx_desc on ${tableName}(description) USING INVERTED PROPERTIES(\"parser\"=\"standard\") COMMENT 'index for description';"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // show index after add index
    show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 3)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[0][4], "id")
    assertEquals(show_result[0][10], "BITMAP")
    assertEquals(show_result[0][11], "'index for id'")
    assertEquals(show_result[0][12], "")
    assertEquals(show_result[1][2], "idx_name")
    assertEquals(show_result[1][4], "name")
    assertEquals(show_result[1][10], "INVERTED")
    assertEquals(show_result[1][11], "'index for name'")
    assertEquals(show_result[1][12], "(\"parser\" = \"none\")")
    assertEquals(show_result[2][2], "idx_desc")
    assertEquals(show_result[2][4], "description")
    assertEquals(show_result[2][10], "INVERTED")
    assertEquals(show_result[2][11], "index for description")
    assertEquals(show_result[2][12], "(\"parser\" = \"standard\")")

    // drop index
    // add index on column description
    sql "drop index idx_name on ${tableName}"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 2)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[0][4], "id")
    assertEquals(show_result[0][10], "BITMAP")
    assertEquals(show_result[0][11], "'index for id'")
    assertEquals(show_result[0][12], "")
    assertEquals(show_result[1][2], "idx_desc")
    assertEquals(show_result[1][4], "description")
    assertEquals(show_result[1][10], "INVERTED")
    assertEquals(show_result[1][11], "index for description")
    assertEquals(show_result[1][12], "(\"parser\" = \"standard\")")

    // add index on column description
    sql "create index idx_name on ${tableName}(name) USING INVERTED COMMENT 'new index for name';"
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // show index after add index
    show_result = sql "show index from ${tableName}"
    logger.info("show index from " + tableName + " result: " + show_result)
    assertEquals(show_result.size(), 3)
    assertEquals(show_result[0][2], "idx_id")
    assertEquals(show_result[0][4], "id")
    assertEquals(show_result[0][10], "BITMAP")
    assertEquals(show_result[0][11], "'index for id'")
    assertEquals(show_result[0][12], "")
    assertEquals(show_result[1][2], "idx_desc")
    assertEquals(show_result[1][4], "description")
    assertEquals(show_result[1][10], "INVERTED")
    assertEquals(show_result[1][11], "index for description")
    assertEquals(show_result[1][12], "(\"parser\" = \"standard\")")
    assertEquals(show_result[2][2], "idx_name")
    assertEquals(show_result[2][4], "name")
    assertEquals(show_result[2][10], "INVERTED")
    assertEquals(show_result[2][11], "new index for name")
    assertEquals(show_result[2][12], "")
}
