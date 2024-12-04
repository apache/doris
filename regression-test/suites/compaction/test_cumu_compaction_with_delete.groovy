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

    GetDebugPoint().clearDebugPointsForAllBEs()
    try {
        GetDebugPoint().enableDebugPointForAllBEs("SizeBaseCumulativeCompactionPolicy.pick_input_rowsets.set_promotion_size_to_max")
        GetDebugPoint().enableDebugPointForAllBEs("TabletManager::find_best_tablets_to_compaction.dcheck")

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

        for(int i = 1; i <= 100; ++i){
            sql """ INSERT INTO ${tableName} VALUES (1,1)"""
            sql """ delete from ${tableName} where user_id = 1"""
            Thread.sleep(500)
        }

        qt_6 """select * from ${tableName} order by user_id, value"""
    } catch (Exception e){
        logger.info(e.getMessage())
        assertFalse(true)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName} FORCE")
        GetDebugPoint().disableDebugPointForAllBEs("SizeBaseCumulativeCompactionPolicy.pick_input_rowsets.set_promotion_size_to_max")
        GetDebugPoint().disableDebugPointForAllBEs("TabletManager::find_best_tablets_to_compaction.dcheck")
    }
}
