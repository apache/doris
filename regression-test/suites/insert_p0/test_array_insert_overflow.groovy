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

suite("test_array_insert_overflow") {
    def testTable = "test_array_insert_overflow"
    sql "ADMIN SET FRONTEND CONFIG ('enable_array_type' = 'true')"
    sql """ set enable_vectorized_engine = true """

    sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` ARRAY<INT> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
    """
    
    test {
        sql "insert into ${testTable} values (1005, [-2147483649])"
        // check exception message contains
        exception "Number out of range"
    }

    test {
        sql "insert into ${testTable} values (1006, [-9223372036854775809])"
        // check exception message contains
        exception "Number out of range"
    }
}