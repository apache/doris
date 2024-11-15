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

suite("test_auto_inc_fetch_fail", "nonConcurrent") {

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
        def table1 = "test_auto_inc_fetch_fail"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k`  int,
                `c1` int,
                `c2` int,
                `c3` int,
                `id` BIGINT NOT NULL AUTO_INCREMENT(10000),
                ) UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1"); """

        GetDebugPoint().enableDebugPointForAllBEs("AutoIncIDBuffer::_fetch_ids_from_fe.failed")

        try {
            sql """insert into ${table1}(k,c1,c2,c3) values(1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4); """
        } catch (Exception e) {
            logger.info("error : ${e}")
        }
        qt_sql "select count(*) from ${table1};"

        GetDebugPoint().clearDebugPointsForAllBEs()

        Thread.sleep(1000)
        
        sql """insert into ${table1}(k,c1,c2,c3) values(1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4); """
        qt_sql "select count(*) from ${table1};"
        qt_sql "select count(*) from ${table1} where id < 10000;"

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
