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
        def result = sql "ADMIN SHOW REPLICA DISTRIBUTION FROM ${tableName} PARTITION ${partitionName}"
        int sum = 0
        for (row in result) {
            sum += row[1].toInteger()
        }
        sum
    }

    assertEquals(1, queryReplicaCount("p1"))

    sql """ ALTER TABLE ${tableName} ADD PARTITION p2 VALUES LESS THAN ("200") """
    assertEquals(1, queryReplicaCount("p2"))

    sql """ ALTER TABLE ${tableName} SET ( "default.replication_allocation" = "tag.location.default: 2" ) """
    sql """ ALTER TABLE ${tableName} ADD PARTITION p3 VALUES LESS THAN ("300") """
    assertEquals(2, queryReplicaCount("p3"))

    sql """ ALTER TABLE ${tableName} MODIFY PARTITION p1 SET ( "replication_allocation" = "tag.location.default: 2" ) """
    for (i = 0; i < 300; i++) {
        if (queryReplicaCount("p1") != 2) {
            Thread.sleep(3000)
        }
    }
    assertEquals(2, queryReplicaCount("p1"))
    assertEquals(1, queryReplicaCount("p2"))

    sql "DROP TABLE ${tableName}"
}

