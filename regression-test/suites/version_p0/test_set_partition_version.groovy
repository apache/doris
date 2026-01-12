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

suite("test_set_partition_version") {
    if (isCloudMode()) {
        return
    }

    def tableName1 = "test_set_partition_version"
    sql """ DROP TABLE IF EXISTS ${tableName1} """
    sql """
    CREATE TABLE ${tableName1} (
       `id` int NOT NULL,
       `version` int NOT NULL COMMENT '插入次数'
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES 
    (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
    );
    """

    def res = sql """ show partitions from ${tableName1}; """
    def partitionId = res[0][0].toString()

    // load 1 time, partition visible version should be 2
    sql """ insert into ${tableName1} values (1, 2); """
    res = sql """ show partitions from ${tableName1}; """
    assertEquals(res[0][2].toString(), "2")

    // load 2 time, partition visible version should be 3
    sql """ insert into ${tableName1} values (2, 3); """
    res = sql """ show partitions from ${tableName1}; """
    assertEquals(res[0][2].toString(), "3")

    // set partition visible version to 2
    sql """ ADMIN SET TABLE ${tableName1} PARTITION VERSION PROPERTIES ("partition_id" = "${partitionId}", "visible_version" = "2"); """
    res = sql """ show partitions from ${tableName1}; """
    assertEquals(res[0][2].toString(), "2")

    // check if table can query, and return row size should be 1
    res = sql """ select * from ${tableName1}; """
    assertEquals(res.size(), 1)

    // set partition visible version to 3
    sql """ ADMIN SET TABLE ${tableName1} PARTITION VERSION PROPERTIES ("partition_id" = "${partitionId}", "visible_version" = "3"); """
    res = sql """ show partitions from ${tableName1}; """
    assertEquals(res[0][2].toString(), "3")

    // check if table can query, and return row size should be 2
    res = sql """ select * from ${tableName1}; """
    assertEquals(res.size(), 2)

    // load 3 time, partition visible version should be 4
    sql """ insert into ${tableName1} values (3, 4); """
    res = sql """ show partitions from ${tableName1}; """
    assertEquals(res[0][2].toString(), "4")
}
