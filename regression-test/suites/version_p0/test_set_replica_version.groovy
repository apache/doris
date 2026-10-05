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

suite("test_set_replica_version") {
    if (isCloudMode()) {
        return
    }
    def tableName1 = "test_set_replica_version"
    sql """ DROP TABLE IF EXISTS ${tableName1} """
    sql """
    CREATE TABLE ${tableName1} (
       `id` int NOT NULL,
       `version` int NOT NULL COMMENT '插入次数'
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES 
    (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
    );
    """

    def res = sql """ show tablets from ${tableName1}; """
    def tabletId = res[0][0].toString()
    def backendId = res[0][2].toString()

    def version = 10
    def lastFailedVersion = 100
    sql """ ADMIN SET REPLICA VERSION PROPERTIES (
        "tablet_id" = "${tabletId}", "backend_id" = "${backendId}",
        "version" = "${version}", "last_failed_version" = "${lastFailedVersion}"
        ); """
    res = sql """ show tablets from ${tableName1}; """
    assertEquals(res[0][4].toString(), "10")
    assertEquals(res[0][5].toString(), "10")
    assertEquals(res[0][6].toString(), "100")

    lastFailedVersion = -1
    sql """ ADMIN SET REPLICA VERSION PROPERTIES (
        "tablet_id" = "${tabletId}", "backend_id" = "${backendId}",
        "last_failed_version" = "${lastFailedVersion}"
        ); """
    res = sql """ show tablets from ${tableName1}; """
    assertEquals(res[0][4].toString(), "10")
    assertEquals(res[0][5].toString(), "10")
    assertEquals(res[0][6].toString(), "-1")

    version = 20
    lastFailedVersion = 100
    sql """ ADMIN SET REPLICA VERSION PROPERTIES (
        "tablet_id" = "${tabletId}", "backend_id" = "${backendId}",
        "version" = "${version}", "last_failed_version" = "${lastFailedVersion}"
        ); """
    res = sql """ show tablets from ${tableName1}; """
    assertEquals(res[0][4].toString(), "20")
    assertEquals(res[0][5].toString(), "20")
    assertEquals(res[0][6].toString(), "100")

    version = 200
    sql """ ADMIN SET REPLICA VERSION PROPERTIES (
        "tablet_id" = "${tabletId}", "backend_id" = "${backendId}",
        "version" = "${version}"
        ); """
    res = sql """ show tablets from ${tableName1}; """
    assertEquals(res[0][4].toString(), "200")
    assertEquals(res[0][5].toString(), "200")
    assertEquals(res[0][6].toString(), "-1")

    sql """ DROP TABLE IF EXISTS ${tableName1} """
}
