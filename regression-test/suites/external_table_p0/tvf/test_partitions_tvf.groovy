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

suite("test_partitions_tvf","p0,external,tvf,external_docker") {
    String suiteName = "test_partitions_tvf"
    String tableName = "${suiteName}_table"
    sql """drop table if exists `${tableName}`"""
    String dbName = context.config.getDbNameByFile(context.file)

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        COMMENT "my first table"
        PARTITION BY LIST(`k3`)
        (
            PARTITION `p1` VALUES IN ('1')
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    order_qt_desc "desc function partitions('catalog'='internal','database'='${dbName}','table'='${tableName}');"
    List<List<Object>> res =  sql """ select * from partitions('catalog'='internal',"database"="${dbName}","table"="${tableName}"); """
    logger.info("res: " + res.toString())

    assertEquals(1, res.size());
    // PartitionName
    assertEquals("p1", res[0][1]);
    // State
    assertEquals("NORMAL", res[0][4]);
    // PartitionKey
    assertEquals("k3", res[0][5]);
    // Buckets
    assertEquals(2, res[0][8]);
    // ReplicationNum: if force_olap_table_replication_num is set to 3,here will be 3
    // assertEquals(1, res[0][9]);
    // StorageMedium
    assertEquals("HDD", res[0][10]);
    // ReplicaAllocation: if force_olap_table_replication_num is set to 3,here will be 3
    // assertEquals("tag.location.default: 1", res[0][16]);
    // IsMutable
    assertEquals(true, res[0][17]);
    // SyncWithBaseTables
    assertEquals(true, res[0][18]);


    // test exception
    test {
        sql """ select * from partitions("catalog"="internal","database"="${dbName}","table"="xxx"); """
        // check exception
        exception "xxx"
    }
    test {
        sql """ select * from partitions("database"="${dbName}"); """
        // check exception
        exception "Invalid"
    }

    sql """drop table if exists `${tableName}`"""
}
