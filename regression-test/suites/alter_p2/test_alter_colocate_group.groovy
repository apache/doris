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

suite ("test_alter_colocate_group") {
    sql "DROP DATABASE IF EXISTS test_alter_colocate_group_db FORCE"
    test {
        sql """
              ALTER COLOCATE GROUP test_alter_colocate_group_db.bad_group_1
              SET ( "replication_num" = "1" );
            """

        exception "unknown databases"
    }
    test {
        sql """
              ALTER COLOCATE GROUP bad_group_2
              SET ( "replication_num" = "1" );
        """

        exception "Not found colocate group `default_cluster:regression_test_alter_p2`.`bad_group_2`"
    }
    test {
        sql """
              ALTER COLOCATE GROUP bad_db.__global__bad_group_3
              SET ( "replication_num" = "1" );
        """

        exception "group that name starts with `__global__` is a global group, it doesn't belong to any specific database"
    }
    test {
        sql """
              ALTER COLOCATE GROUP __global__bad_group_4
              SET ( "replication_num" = "1" );
        """

        exception "Not found colocate group `__global__bad_group_4`"
    }

    sql " DROP TABLE IF EXISTS tbl1 FORCE; "
    sql " DROP TABLE IF EXISTS tbl2 FORCE; "
    sql " DROP TABLE IF EXISTS tbl3 FORCE; "

    sql """
        CREATE TABLE tbl1
        (
            k1 int,
            k2 int
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 6
        PROPERTIES
        (
            "colocate_with" = "group_1",
            "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE tbl2
        (
            k1 date,
            k2 int
        )
        PARTITION BY RANGE(k1)
        (
            PARTITION p1 values less than('2020-02-01'),
            PARTITION p2 values less than('2020-03-01')
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 5
        PROPERTIES
        (
            "colocate_with" = "group_2",
            "replication_num" = "1"
        );
    """

    sql """
        CREATE TABLE tbl3
        (
            `uuid` varchar(255) NULL,
            `action_datetime` date NULL
        )
        DUPLICATE KEY(uuid)
        PARTITION BY RANGE(action_datetime)()
        DISTRIBUTED BY HASH(uuid) BUCKETS 4
        PROPERTIES
        (
            "colocate_with" = "group_3",
            "replication_num" = "1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.end" = "2",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "4",
            "dynamic_partition.replication_num" = "1"
         );
    """

    def checkGroupsReplicaAlloc = { groupName, replicaNum ->
        // groupName -> replicaAlloc
        def allocMap = [:]
        def groups = sql """ show proc "/colocation_group" """
        for (def group : groups) {
            allocMap[group[1]] = group[4]
        }

        assertEquals("tag.location.default: ${replicaNum}".toString(), allocMap[groupName])
    }

    def checkTableReplicaAlloc = { tableName, hasDynamicPart, replicaNum ->
        def result = sql """ show create table ${tableName} """
        def createTbl = result[0][1].toString()
        assertTrue(createTbl.indexOf("\"replication_allocation\" = \"tag.location.default: ${replicaNum}\"") > 0)
        if (hasDynamicPart) {
            assertTrue(createTbl.indexOf(
                    "\"dynamic_partition.replication_allocation\" = \"tag.location.default: ${replicaNum}\"") > 0)
        }

        result = sql """ show partitions from ${tableName} """
        assertTrue(result.size() > 0)
        for (int i = 0; i < result.size(); i++) {
            assertEquals("${replicaNum}".toString(), result[i][9].toString())
        }
    }

    for (int i = 1; i <= 3; i++) {
        def groupName = "regression_test_alter_p2.group_${i}"
        checkGroupsReplicaAlloc(groupName, 1)

        def tableName = "tbl${i}"
        def hasDynamicPart = i == 3
        checkTableReplicaAlloc(tableName, hasDynamicPart, 1)

        test {
            sql """
              ALTER COLOCATE GROUP ${groupName}
              SET ( "replication_num" = "100" );
            """

            exception "Failed to find enough host"
        }

        test {
            sql """
              ALTER COLOCATE GROUP ${groupName}
              SET ( "replication_num" = "3" );
            """
        }

        checkGroupsReplicaAlloc(groupName, 3)
        checkTableReplicaAlloc(tableName, hasDynamicPart, 3)
    }

    sql " DROP TABLE IF EXISTS tbl1 FORCE; "
    sql " DROP TABLE IF EXISTS tbl2 FORCE; "
    sql " DROP TABLE IF EXISTS tbl3 FORCE; "
}
