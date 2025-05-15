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

suite("test_partial_update_memtbl_flush", "p0,nonConcurrent") {
    def tableName = "test_partial_update_memtbl_flush_t"
    def fileName = "random_data"

    try {

        sql """ DROP TABLE IF EXISTS ${tableName} """

        sql """
            CREATE TABLE ${tableName} (
                `id` int,
                `k` int,
                `s` varchar(80)
            )
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        // limit the max string size to 7900000
        GetDebugPoint().enableDebugPointForAllBEs("ColumnString.check_string_overflow", [max_string_size:"7900000"])
        GetDebugPoint().enableDebugPointForAllBEs("MemTable.update_flush_max_size", [max_size:"30000"])

        // steam load data 100000 rows, each row has 80 bytes string
        // memtable will flush according to WA
        for (int i = 1; i <= 10; i++) {
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                file "${fileName}_${i}.csv"
                time 10000
            }
        }

        // partial update 100000 rows, total 8000000 bytes
        // replenish 8000000 bytes string when memtable flushing
        sql """
            UPDATE ${tableName} SET k = 20
        """

        def result = sql """
            select count(*) from ${tableName} where k = 20;
        """

        logger.info("result: " + result.toString())

        assertEquals(result[0][0], 100000)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("ColumnString.check_string_overflow")
        GetDebugPoint().disableDebugPointForAllBEs("MemTable.update_flush_max_size")
    }
}