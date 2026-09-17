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

suite("test_uuid_dynamic_partition") {
    sql "DROP TABLE IF EXISTS uuid_dynamic_partition_rejected"
    for (enableProperty in ['"dynamic_partition.enable" = "true",',
                            '"dynamic_partition.enable" = "false",', '']) {
        test {
            sql """
                CREATE TABLE uuid_dynamic_partition_rejected (
                    id INT,
                    uuid_col UUID NOT NULL
                )
                DUPLICATE KEY(id)
                PARTITION BY RANGE(uuid_col)()
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    ${enableProperty}
                    "dynamic_partition.time_unit" = "DAY",
                    "dynamic_partition.start" = "-1",
                    "dynamic_partition.end" = "1",
                    "dynamic_partition.prefix" = "p",
                    "dynamic_partition.buckets" = "1",
                    "dynamic_partition.create_history_partition" = "true"
                )
            """
            exception "Dynamic Partition Only Support DATE, DATETIME and INTEGER Type Now."
        }
    }
    order_qt_rejected_table "SHOW TABLES LIKE 'uuid_dynamic_partition_rejected'"

    sql "DROP TABLE IF EXISTS uuid_static_partition"
    sql """
        CREATE TABLE uuid_static_partition (
            id INT,
            uuid_col UUID NOT NULL
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(uuid_col) (
            PARTITION p_all VALUES LESS THAN (MAXVALUE)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO uuid_static_partition VALUES (1, '00000000-0000-0000-0000-000000000001')"
    test {
        sql """
            ALTER TABLE uuid_static_partition SET (
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-1",
                "dynamic_partition.end" = "1",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "1",
                "dynamic_partition.create_history_partition" = "true"
            )
        """
        exception "Dynamic Partition Only Support DATE, DATETIME and INTEGER Type Now."
    }
    sql "INSERT INTO uuid_static_partition VALUES (2, '00000000-0000-0000-0000-000000000002')"
    order_qt_static_uuid "SELECT id, uuid_col FROM uuid_static_partition"
    def uuidDynamicTables = sql_return_maparray("SHOW DYNAMIC PARTITION TABLES").findAll {
        it.TableName in ['uuid_dynamic_partition_rejected', 'uuid_static_partition']
    }
    qt_uuid_not_registered "SELECT ${uuidDynamicTables.size()}"

    sql "DROP TABLE IF EXISTS uuid_dynamic_partition_date_control"
    sql """
        CREATE TABLE uuid_dynamic_partition_date_control (
            id INT,
            date_col DATE NOT NULL
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(date_col)()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.start" = "-1",
            "dynamic_partition.end" = "1",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "1",
            "dynamic_partition.create_history_partition" = "true"
        )
    """
    def datePartitions = sql "SHOW PARTITIONS FROM uuid_dynamic_partition_date_control"
    def dateDynamicInfo = sql_return_maparray("SHOW DYNAMIC PARTITION TABLES").find {
        it.TableName == 'uuid_dynamic_partition_date_control'
    }
    qt_date_partitions "SELECT ${datePartitions.size()}, '${dateDynamicInfo.State}'"
    sql "INSERT INTO uuid_dynamic_partition_date_control VALUES (1, CURRENT_DATE())"
    order_qt_date_insert "SELECT id FROM uuid_dynamic_partition_date_control"
    sql "ALTER TABLE uuid_dynamic_partition_date_control SET ('dynamic_partition.end' = '2')"
    sql "ALTER TABLE uuid_dynamic_partition_date_control SET ('dynamic_partition.enable' = 'false')"
    sql "ALTER TABLE uuid_dynamic_partition_date_control SET ('dynamic_partition.enable' = 'true')"
    sql "INSERT INTO uuid_dynamic_partition_date_control VALUES (2, CURRENT_DATE())"
    order_qt_date_alter "SELECT id FROM uuid_dynamic_partition_date_control"
}
