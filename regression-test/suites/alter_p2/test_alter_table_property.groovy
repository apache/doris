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

suite ("test_alter_table_property") {
    if (isCloudMode()) {
        return
    }

    String tableName = "test_alter_table_property_table"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            id LARGEINT NOT NULL,
            value LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("100")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default: 1"
        )
        """
    sql "INSERT INTO ${tableName} VALUES (50, 100)"

    def queryReplicaCount = { partitionName ->
        def result = sql "SHOW REPLICA DISTRIBUTION FROM ${tableName} PARTITION ${partitionName}"
        logger.info("${result}")
        int sum = 0
        for (def row in result) {
            sum += row[1].toInteger()
        }
        sum
    }
    def replication_num = 1
    def forceReplicaNum = getFeConfig('force_olap_table_replication_num').toInteger()
    if (forceReplicaNum > 0) {
        replication_num = forceReplicaNum
    }

    assertEquals(replication_num, queryReplicaCount("p1"))

    sql """ ALTER TABLE ${tableName} ADD PARTITION p2 VALUES LESS THAN ("200") """
    assertEquals(replication_num, queryReplicaCount("p2"))

    def res = sql """show backends;"""
    if (res.size() < 3) {
        return
    }

    sql """ ALTER TABLE ${tableName} SET ( "default.replication_allocation" = "tag.location.default: 2" ) """
    sql """ ALTER TABLE ${tableName} ADD PARTITION p3 VALUES LESS THAN ("300") """
    assertEquals(2, queryReplicaCount("p3"))

    sql """ ALTER TABLE ${tableName} MODIFY PARTITION p1 SET ( "replication_allocation" = "tag.location.default: 2" ) """
    for (def i = 0; i < 300; i++) {
        if (queryReplicaCount("p1") != 2) {
            Thread.sleep(3000)
        }
    }
    assertEquals(2, queryReplicaCount("p1"))
    assertEquals(replication_num, queryReplicaCount("p2"))

    sql """ ALTER TABLE ${tableName} SET("storage_medium"="SSD") """

    def result = sql_return_maparray """
    show create table ${tableName}
    """
    logger.info(${result[0]})
    def createTableStr = result[0]['Create Table']
    assertTrue(createTableStr.contains("\"storage_medium\" = \"ssd\""))

    // Test medium_allocation_mode property
    sql """ ALTER TABLE ${tableName} SET("medium_allocation_mode"="strict") """

    def result2 = sql_return_maparray """
    show create table ${tableName}
    """
    logger.info(${result2[0]})
    def createTableStr2 = result2[0]['Create Table']
    assertTrue(createTableStr2.contains("\"medium_allocation_mode\" = \"strict\""))

    // Test setting medium_allocation_mode to adaptive
    sql """ ALTER TABLE ${tableName} SET("medium_allocation_mode"="adaptive") """

    def result3 = sql_return_maparray """
    show create table ${tableName}
    """
    logger.info(${result3[0]})
    def createTableStr3 = result3[0]['Create Table']
    assertTrue(createTableStr3.contains("\"medium_allocation_mode\" = \"adaptive\""))

    sql "DROP TABLE ${tableName}"
}

