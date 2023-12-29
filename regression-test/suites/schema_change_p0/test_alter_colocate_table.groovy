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

suite ("test_alter_colocate_table") {
    def tbls = ["col_tbl1", "col_tbl2", "col_tbl3"]
    for (def tbl : tbls) {
        sql """
            DROP TABLE IF EXISTS ${tbl} FORCE
        """
    }

    // wait group clean
    def existsGroup = false;
    for (def i = 0; i < 60; i++) {
        def dbName = sql("SELECT DATABASE()")[0][0]
        def result = sql_return_maparray "show proc '/colocation_group'"
        existsGroup = result.any { it.GroupName.indexOf(dbName + ".x_group_") >= 0 }
        if (existsGroup) {
            sleep(1000)
        } else {
            break
        }
    }
    assertFalse(existsGroup)

    def forceReplicaNum = getFeConfig('force_olap_table_replication_num') as int
    def replicaNum = forceReplicaNum > 0 ? forceReplicaNum : 1

    sql """
        CREATE TABLE IF NOT EXISTS col_tbl1
        (
           k1 date,
           k2 int
        )
        ENGINE=OLAP
        UNIQUE KEY (k1,k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 3
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default:${replicaNum}",
            "colocate_with" = 'x_group_1'
        )
    """

    sql """
        CREATE TABLE IF NOT EXISTS col_tbl2
        (
           k1 date,
           k2 int
        )
        ENGINE=OLAP
        UNIQUE KEY (k1,k2)
        PARTITION BY RANGE(k1)
        (
           PARTITION p1 values less than('2020-02-01'),
           PARTITION p2 values less than('2020-03-01')
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 3
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default:${replicaNum}",
            "colocate_with" = 'x_group_2'
        )
    """

    sql """
        CREATE TABLE col_tbl3
        (
         `uuid` varchar(255) NULL,
         `action_datetime` date NULL
        )
        DUPLICATE KEY(uuid)
        PARTITION BY RANGE(action_datetime)()
        DISTRIBUTED BY HASH(uuid) BUCKETS 3
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default:${replicaNum}",
            "colocate_with" = "x_group_3",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.end" = "3",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "3",
            "dynamic_partition.replication_num" = "1",
            "dynamic_partition.create_history_partition"= "true",
            "dynamic_partition.replication_allocation" = "tag.location.default:${replicaNum}",
            "dynamic_partition.start" = "-3"
         );
    """

    def errMsg = "Cannot change replication allocation of colocate table"

    test {
        sql """
           ALTER TABLE col_tbl1 set (
                "replication_allocation" = "tag.location.default:${replicaNum}"
           )
        """
        exception errMsg
    }

    test {
        sql """
           ALTER TABLE col_tbl3 set (
                "dynamic_partition.replication_allocation" = "tag.location.default:${replicaNum}"
           )
        """
        exception errMsg
    }

    for (def tbl : tbls) {
        test {
            sql """
              ALTER TABLE ${tbl} set (
                "default.replication_allocation" = "tag.location.default:${replicaNum}"
              )
            """

            exception errMsg
        }

        test {
            sql """
              ALTER TABLE ${tbl} MODIFY PARTITION (*) set (
                "replication_allocation" = "tag.location.default:${replicaNum}"
              )
            """

            exception errMsg
        }

        sql "DROP TABLE ${tbl} FORCE"
    }
}

