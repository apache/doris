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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

suite("test_auto_inc_schema_change_double_write", "nonConcurrent") {

    def table1 = "test_auto_inc_schema_change_double_write"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` BIGINT NOT NULL AUTO_INCREMENT,
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"); """

    sql """insert into ${table1} select number,number,number,number,number from numbers("number"="5000"); """
    sql "sync;"
    qt_sql "select count(*) from ${table1} group by k1 having count(*) > 1;"

    def run_test = {thread_num, rows, iters -> 
        def threads = []
        (1..thread_num).each { id1 -> 
            threads.add(Thread.start {
                (1..iters).each { id2 -> 
                    sql """insert into ${table1}(value) select number from numbers("number" = "${rows}");"""
                }
            })
        }

        threads.each { thread -> thread.join() }
    }

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        
        AtomicBoolean stopped = new AtomicBoolean(false)
        def t1 = Thread.start {
            while (!stopped.get()) {
                run_test(5, 1000, 3)
                Thread.sleep(100)
            }
        }

        Thread.sleep(3000)

        sql "alter table ${table1} modify column c3 varchar(100) null;"

        Thread.sleep(3000);

        GetDebugPoint().disableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")

        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1"""
            time 20000
        }

        stopped.set(true)
        t1.join()

        qt_sql "select count(*) from ${table1} group by k1 having count(*) > 1;"

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    sql "DROP TABLE IF EXISTS ${table1};"
}
