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

suite("test_cumu_compaction_with_delete") {
    def tableName = "test_cumu_compaction_with_delete"
    def check_cumu_point = { cumu_point ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        int cumuPoint = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            cumuPoint = tabletJson["cumulative point"]
        }
        return cumuPoint == cumu_point
    }

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
            `user_id` INT NOT NULL,
            `value` INT NOT NULL)
            UNIQUE KEY(`user_id`) 
            DISTRIBUTED BY HASH(`user_id`) 
            BUCKETS 1 
            PROPERTIES ("replication_allocation" = "tag.location.default: 1",
            "enable_mow_light_delete" = "true")"""

        for(int i = 1; i <= 50; ++i){
            sql """ INSERT INTO ${tableName} VALUES (1,1)"""
            sql """ delete from ${tableName} where user_id = 1"""
        }

        now = System.currentTimeMillis()

        while(true){
            if(check_cumu_point(102)){
                break;
            }
            Thread.sleep(1000)
        }
        time_diff = System.currentTimeMillis() - now
        logger.info("time_diff:" + time_diff)
        assertTrue(time_diff<250*1000)

        qt_select """select * from ${tableName} order by user_id, value"""
    } catch (Exception e){
        logger.info(e.getMessage())
        assertFalse(true)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName} FORCE")
    }
}