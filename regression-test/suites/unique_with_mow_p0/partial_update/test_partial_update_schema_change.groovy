
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

suite("test_partial_update_schema_change", "p0") {
    def tableName = "test_partial_update_schema_change"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NOT NULL,
                `c1` int NOT NULL,
                `c2` int NOT NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "true",
                    "enable_unique_key_merge_on_write" = "true")
    """
    sql " insert into ${tableName} values(1,1,1) "

    sql " insert into ${tableName} values(2,2,2) "

    qt_sql " select * from ${tableName} order by c0 "

    sql " ALTER table ${tableName} add column c3 INT DEFAULT '0' "

    sql " update ${tableName} set c1 = 3 where c0 = 1 "

    qt_sql " select * from ${tableName} order by c0 "

    sql " ALTER table ${tableName} drop column c3 "

    sql " update ${tableName} set c1 = 4 where c0 = 1 "

    qt_sql " select * from ${tableName} order by c0 "

    // drop table
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
