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
    def backends = sql_return_maparray('show backends')
    def replicaNum = 0
    for (def be : backends) {
        def alive = be.Alive.toBoolean()
        def decommissioned = be.SystemDecommissioned.toBoolean()
        if (alive && !decommissioned) {
            replicaNum++
        }
    }
    assertTrue(replicaNum > 0)
    if (isCloudMode()) {
        replicaNum = 1
    }

    def block_convert_historical_rowsets = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        }
    }

    def unblock = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        }
    }

    for (def model : ["UNIQUE", "DUPLICATE"]) {
        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()
            def tableName = "test_auto_inc_schema_change_double_write"
            def table1 = "${tableName}_${model}"
            sql "DROP TABLE IF EXISTS ${table1} FORCE;"
            sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                    `k1` BIGINT NOT NULL AUTO_INCREMENT,
                    `c1` int,
                    `c2` int,
                    `c3` int,
                    `c4` int
                    )${model} KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "disable_auto_compaction" = "true",
                    "replication_num" = "${replicaNum}"); """

            sql """insert into ${table1}(c1,c2,c3,c4) select number,number,number,number from numbers("number"="5000"); """
            sql "sync;"
            qt_sql "select count(*) from ${table1} group by k1 having count(*) > 1;"

            block_convert_historical_rowsets()
            
            AtomicBoolean stopped = new AtomicBoolean(false)

            def iters = 3
            def rows = 500
            def thread_num = 4
            def t1 = Thread.start {
                def threads = []
                (1..thread_num).each { id1 -> 
                    threads.add(Thread.start {
                        while (!stopped.get()) {
                            (1..iters).each { id2 -> 
                                sql """insert into ${table1}(c1,c2,c3,c4) select number,number,number,number from numbers("number"="${rows}");"""
                            }
                            Thread.sleep(200)
                        }
                    })
                }

                threads.each { thread -> thread.join() }
            }

            Thread.sleep(3000)

            sql "alter table ${table1} modify column c3 varchar(100) null;"

            Thread.sleep(3000);

            unblock()

            def t2 = Thread.start {
                waitForSchemaChangeDone {
                    sql """SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1"""
                    time 20000
                }
            }
            
            Thread.sleep(5000);
            stopped.set(true)
            t1.join()
            t2.join()

            qt_sql "select count(*) from ${table1} group by k1 having count(*) > 1;"

        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
