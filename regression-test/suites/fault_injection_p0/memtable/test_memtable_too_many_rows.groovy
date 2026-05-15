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

suite("test_memtable_too_many_rows", "nonConcurrent") {
    GetDebugPoint().clearDebugPointsForAllBEs()
    def testTable = "test_memtable_too_many_rows"
    sql """ DROP TABLE IF EXISTS ${testTable}"""

    sql """
        CREATE TABLE IF NOT EXISTS `${testTable}` (
          `id` BIGINT NOT NULL,
          `value` int(11) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
    """

    def debugPoint = "MemTableWriter.too_many_raws"
    try {
        GetDebugPoint().enableDebugPointForAllBEs(debugPoint)
        sql "insert into ${testTable} values(1,1)"
        def res = sql "select * from ${testTable}"
        logger.info("res: " + res.size())
        assertTrue(res.size() == 1)
    } catch (Exception e){
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("write memtable too many rows fail"))
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(debugPoint)
    }
}