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


suite("test_nereids_show_replica_dist") {
    def tableName = "test_nereids_show_replica_dist"
    // create table and insert data
    sql """ drop table if exists ${tableName}"""
    sql """
    create table ${tableName} (
        `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    partition by range(da)(
        PARTITION p3 VALUES LESS THAN ('2023-01-01'),
        PARTITION p4 VALUES LESS THAN ('2024-01-01'),
        PARTITION p5 VALUES LESS THAN ('2025-01-01')
    )
    distributed by hash(id) buckets 1
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """

    def queryReplicaCount = { partitionName ->
        def result = sql "SHOW REPLICA DISTRIBUTION FROM ${tableName} PARTITION ${partitionName}"
        logger.info("${result}")
        int sum = 0
        for (row in result) {
            sum += row[1].toInteger()
        }
        sum
    }

    def queryAdminReplicaCount = { partitionName ->
        def result = sql "ADMIN SHOW REPLICA DISTRIBUTION FROM ${tableName} PARTITION ${partitionName}"
        logger.info("${result}")
        int sum = 0
        for (row in result) {
            sum += row[1].toInteger()
        }
        sum
    }
    
    def replication_num = 1
    def forceReplicaNum = getFeConfig('force_olap_table_replication_num').toInteger()
    if (forceReplicaNum > 0) {
        replication_num = forceReplicaNum
    }

    assertEquals(replication_num, queryReplicaCount("p3"))   
    assertEquals(replication_num, queryReplicaCount("p4"))   
    assertEquals(replication_num, queryReplicaCount("p5"))           

    assertEquals(replication_num, queryAdminReplicaCount("p3"))   
    assertEquals(replication_num, queryAdminReplicaCount("p4"))   
    assertEquals(replication_num, queryAdminReplicaCount("p5"))  

    checkNereidsExecute("SHOW REPLICA DISTRIBUTION FROM ${tableName}")
    sql "SHOW REPLICA DISTRIBUTION FROM ${tableName}"
    checkNereidsExecute("ADMIN SHOW REPLICA DISTRIBUTION FROM ${tableName}")
    sql "ADMIN SHOW REPLICA DISTRIBUTION FROM ${tableName}"    
    sql """ drop table if exists ${tableName}"""

}

