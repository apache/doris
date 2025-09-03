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

import org.apache.doris.regression.suite.Suite

def delta_time = 1000

Suite.metaClass.wait_for_last_build_index_finish = {table_name, OpTimeout ->
    def useTime = 0
    for(int t = delta_time; t <= OpTimeout; t += delta_time){
        def alter_res = sql """show build index order by CreateTime desc limit 1;"""
        alter_res = alter_res.toString()
        if(alter_res.contains("FINISHED")) {
            sleep(3000) // wait change table state to normal
            logger.info(table_name + " latest alter job finished, detail: " + alter_res)
            break
        } else if (alter_res.contains("CANCELLED")) {
            logger.info(table_name + " latest alter job failed, detail: " + alter_res)
            assertTrue(false)
        }
        useTime = t
        sleep(delta_time)
    }
    assertTrue(useTime <= OpTimeout, "wait for last build index finish timeout")
}

Suite.metaClass.wait_for_latest_col_change_table_finish = { table_name, OpTimeout ->
    def useTime = 0

    for (int t = delta_time; t <= OpTimeout; t += delta_time) {
        def alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
        alter_res = alter_res.toString()
        if (alter_res.contains("FINISHED")) {
            sleep(3000) // wait change table state to normal
            logger.info(table_name + " latest alter job finished, detail: " + alter_res)
            break
        }
        useTime = t
        sleep(delta_time)
    }
    assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
}