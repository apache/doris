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

suite("regression_test_variant_concurrent_schema_update", ""){
    def table_name = "var_concurrent"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant,
            v1 variant 
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1");
    """
    t1 = Thread.startDaemon {
        for (int k = 1; k <= 60; k++) {
            int x = k % 10;
            sql """insert into ${table_name} values(${x}, '{"k${x}" : ${x}, "x${k}" : 123}', '{"k${x}" : ${x}, "x${k}" : 123}')"""
        } 
    }
    t2 = Thread.startDaemon {
        for (int k = 61; k <= 120; k++) {
            int x = k % 10;
            sql """insert into ${table_name} values(${x}, '{"k${x}" : ${x}, "x${k}" : 123}', '{"k${x}" : ${x}, "x${k}" : 123}')"""
        }
    }
    t3 = Thread.startDaemon {
        for (int k = 121; k <= 180; k++) {
            int x = k % 10;
            sql """insert into ${table_name} values(${x}, '{"k${x}" : ${x}, "x${k}" : 123}', '{"k${x}" : ${x}, "x${k}" : 123}')"""
        }
    }
    t1.join()
    t2.join()
    t3.join()
    sql "sync"
    qt_sql_1 "select * from var_concurrent order by k, cast(v as string), cast(v1 as string) limit 100"
    // qt_sql_3 """desc ${table_name}"""
}