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

import java.util.Random;

// This test is to test compatibility of property "replication_num" in cloud mode
// it should be ignored
suite("test_mtmv_n_replicas") {
    def tableName = "mtmv_n_replicas_table"
    def mvName = "mv_n_replicas"
    def dbName = "regression_test_mtmv_p0"
    def numReplicas = isCloudMode() ? new Random().nextInt(10) /* including 0 */ : 1

    sql """ create database if not exists ${dbName}; """

    sql """ drop table if exists ${dbName}.${tableName}; """
    sql """
        CREATE TABLE ${dbName}.${tableName} (
        id INT,
        name varchar(255),
        score INT SUM
        )
        AGGREGATE KEY(id, name)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        properties ("replication_num" = "${numReplicas}")
        """

    sql """ insert into ${dbName}.${tableName} (id, name, score) values (1, "aaa", 11); """
    sql """ insert into ${dbName}.${tableName} (id, name, score) values (1, "aaa", 11); """
    sql """ insert into ${dbName}.${tableName} (id, name, score) values (2, "aaa", 12); """
    sql """ insert into ${dbName}.${tableName} (id, name, score) values (2, "aaa", 12); """

    sql """ drop materialized view if exists ${dbName}.${mvName}; """
    sql """
        create materialized view ${dbName}.${mvName}
        build immediate refresh complete on manual distributed by random buckets 15
        properties ("replication_num" = "${numReplicas}")
        as select name,id,score from ${dbName}.${tableName};
        """

    sql """ refresh materialized view ${dbName}.${mvName} complete; """

    def n = 100
    while (n > 0) {
        n = n - 1
        sleep(1000)
        def result = sql_return_maparray """ select * from ${dbName}.${mvName} order by id"""
        logger.info("result: ${result}")
        if (result.size() == 2) {
            // [score:22, name:aaa, id:1], [score:24, name:aaa, id:2]
            assertEquals(result[0].id, 1)
            assertEquals(result[0].name, "aaa")
            assertEquals(result[0].score, 22)
            assertEquals(result[1].id, 2)
            assertEquals(result[1].name, "aaa")
            assertEquals(result[1].score, 24)
            break;
        } else if (n < 0) {
            assertEquals(result.size(), 2)
        }
    }
}
