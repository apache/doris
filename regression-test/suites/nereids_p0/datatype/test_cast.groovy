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
    sql """ set enable_nereids_dml=true;"""
    sql """ DROP TABLE IF EXISTS replicationtesttable;"""
    sql """
        CREATE TABLE IF NOT EXISTS replicationtesttable (
            `k1` varchar(144) NOT NULL COMMENT '_key__',
            `k2` int(11) NOT NULL COMMENT '_key__',
            v4 decimal(18,4) null,
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
    test {
        sql """insert into replicationtesttable (k1,k2,v4) select 'aaa',10,'22500000900.000000000000000000000000000000';"""
        exception "Invalid precision and scale - expect (18, 4), but (41, 30)";
    }
    test {
        sql """insert into replicationtesttable (k1,k2,v4) select 'aaa',10,22500000900.000000000000000000000000000000;"""
        exception "Wrong precision 41, min: 1, max: 38";
    }
}