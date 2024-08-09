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

suite("test_boolean_cast", "datatype_p0") {
    def tableName = "test_boolean_cast"
    sql """DROP TABLE IF EXISTS ${tableName}"""

    sql """CREATE  TABLE if NOT EXISTS ${tableName} (
        k int, v0 int, v1 bigint, v2 char(100), v3 date,
        v4 datetime, v5 double, v6 decimal(10,2), v7 ipv4
        ) DUPLICATE KEY(k) DISTRIBUTED BY HASH (k) BUCKETS 1 PROPERTIES ('replication_num' = '1');"""
    
    sql """insert into ${tableName} select 1, 1024, 1073741824, 'abc', '2020-01-01', '2020-01-01 10:10:10', 1024.1024, 1024.1024, '127.0.0.1';"""
    sql """insert into ${tableName} select 2, 0, 0, '0', '1970-01-01', '1970-01-01 00:00:00', 0.00, 0.00, '0.0.0.0';"""

    qt_select """select k, cast(v0 as boolean), cast(v1 as boolean),
    cast(v2 as boolean), cast(v3 as boolean), cast(v4 as boolean), 
    cast(v5 as boolean), cast(v6 as boolean), cast(v7 as boolean) from ${tableName} order by 1;"""


    sql """DROP TABLE IF EXISTS ${tableName}"""
}
