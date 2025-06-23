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

suite("test_memtable_write_failed", "nonConcurrent") {
    GetDebugPoint().clearDebugPointsForAllBEs()
    def testTable = "test_memtable_write_failed"
    sql """ DROP TABLE IF EXISTS ${testTable}"""

    sql """
        CREATE TABLE IF NOT EXISTS `${testTable}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT,
          `value` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """
    
    def run_test = {thread_num, rows, iters -> 
        def threads = []
        (1..thread_num).each { id1 -> 
            threads.add(Thread.start {
                (1..iters).each { id2 -> 
                    try {
                         sql """insert into ${testTable}(value) select number from numbers("number" = "${rows}");"""
                         String content = ""
                         (1..4096).each {
                             content += "${it},${it}\n"
                         }
                         content += content
                         streamLoad {
                             table "${testTable}"
                             set 'column_separator', ','
                             inputStream new ByteArrayInputStream(content.getBytes())
                             time 30000 // limit inflight 10s

                             check { result, exception, startTime, endTime ->
                                 if (exception != null) {
                                     throw exception
                                 }
                                 def json = parseJson(result)
                                 if (json.Status.equalsIgnoreCase("success")) {
                                     assertEquals(8192, json.NumberTotalRows)
                                     assertEquals(0, json.NumberFilteredRows)
                                 } else {
                                     assertTrue(json.Message.contains("write memtable random failed for debug"))
                                 }
                             }
                         }
                    } catch (Exception e) {
                         logger.info(e.getMessage())
                         assertTrue(e.getMessage().contains("write memtable random failed for debug"))
                    }
                }
            })
        }
        threads.each { thread -> thread.join() }
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("MemTableWriter.write.random_insert_error")
        GetDebugPoint().enableDebugPointForAllBEs("MemTableMemoryLimiter._handle_memtable_flush.limit_reached")
        GetDebugPoint().enableDebugPointForAllBEs("MemTableMemoryLimiter._need_flush.random_flush")
        run_test(5, 10000, 10)
    } catch (Exception e){
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("write memtable random failed for debug"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("MemTableWriter.write.random_insert_error")
        GetDebugPoint().disableDebugPointForAllBEs("MemTableMemoryLimiter._handle_memtable_flush.limit_reached")
        GetDebugPoint().disableDebugPointForAllBEs("MemTableMemoryLimiter._need_flush.random_flush")
    }
}
