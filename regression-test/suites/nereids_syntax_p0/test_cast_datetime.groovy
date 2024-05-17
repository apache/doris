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

suite("test_cast_datetime") {

    sql "drop table if exists casttbl"
    sql """CREATE TABLE casttbl ( 
        mydate date NULL,
        mydatev2 DATEV2 null,
        mydatetime datetime null,
        mydatetimev2 datetimev2 null
    ) ENGINE=OLAP
    DUPLICATE KEY(`mydate`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`mydate`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false"
    );
    """

    sql "insert into casttbl values ('2000-01-01', '2000-01-01', '2000-01-01', '2000-01-01 12:12:12');"

    sql "set enable_nereids_planner=true;"

//when BE storage support 'Date < Date', we should remove this case
//currently, if we rewrite expr to CAST(mydatetime AS DATE) < date '2019-06-01', BE returns 0 tuple. 
    qt_1 "select count(1) from casttbl where CAST(CAST(mydatetime AS DATE) AS DATETIME) < date '2019-06-01';"
}