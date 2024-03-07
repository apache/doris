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

suite("version_p0", "p0") {
    def tableName1 = "test_partition_version"
    sql """ DROP TABLE IF EXISTS ${tableName1} """
    sql """
    CREATE TABLE ${tableName1} (
       `TIME_STAMP` datev2 NOT NULL COMMENT '采集日期'
    ) ENGINE=OLAP
    DUPLICATE KEY(`TIME_STAMP`)
    COMMENT 'OLAP'
    PARTITION BY RANGE(`TIME_STAMP`)
    (
        PARTITION p20221220 VALUES [('2022-12-20'), ('2022-12-21')),
        PARTITION p20221221 VALUES [('2022-12-21'), ('2022-12-22'))
    )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
    PROPERTIES 
    (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """ insert into ${tableName1} values ('2022-12-20'); """
    def res = sql """ show partitions from ${tableName1}; """

    // check written partition version is higher
    assertEquals(res[0][2].toString(), "2")
    assert res[1][2].toString() == "1" || \
        res[1][2].toString() == "0"  // Cloud will returns zero.
}