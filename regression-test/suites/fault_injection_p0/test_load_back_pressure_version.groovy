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

suite("test_load_back_pressure_version", "nonConcurrent") {
    sql """ set enable_memtable_on_sink_node=false """
    def testTable = "test_load_back_pressure_version"
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

    def test_load_back_pressure_version = { int targetRows ->
        try {
            set_be_param("load_back_pressure_version_threshold", "0")
            sql "insert into ${testTable} values(1,1)"
            def res = sql "select * from ${testTable}"
            logger.info("res: " + res.size())
            assertTrue(res.size() == targetRows)
        } finally {
            set_be_param("load_back_pressure_version_threshold", "80")
        }
    }

    test_load_back_pressure_version(1)
    sql """ set enable_memtable_on_sink_node=true """
    test_load_back_pressure_version(2)
}