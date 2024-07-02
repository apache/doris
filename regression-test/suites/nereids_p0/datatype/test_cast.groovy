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

suite("test_cast") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    def tbl = "test_cast"

    sql """ DROP TABLE IF EXISTS ${tbl}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tbl} (
            `k0` int
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """ INSERT INTO ${tbl} VALUES (101);"""

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 1 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 12 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then -12 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 0 else 1 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 != 101 then 0 else 1 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then '1' else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then '12' else 0 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 'false' else 0 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 'true' else 1 end"
        result([[101]])
    }

    test {
        sql "select cast(false as datetime);"
        result([[null]])
    }

    test {
        sql "select cast(true as datetime);"
        result([[null]])
    }

    test {
        sql "select cast(false as date);"
        result([[null]])
    }

    test {
        sql "select cast(true as date);"
        result([[null]])
    }
    sql """ DROP TABLE IF EXISTS table_decimal38_4;"""
    sql """
        CREATE TABLE IF NOT EXISTS table_decimal38_4 (
            `k0` decimal(38, 4)
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1");
        """

    sql """ DROP TABLE IF EXISTS table_decimal27_9;"""
    sql """
        CREATE TABLE IF NOT EXISTS table_decimal27_9 (
            `k0` decimal(27, 9)
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1");
        """
    explain {
        sql """select k0 from table_decimal38_4 union all select k0 from table_decimal27_9;"""
        contains """AS DECIMALV3(38, 4)"""
    }
}
