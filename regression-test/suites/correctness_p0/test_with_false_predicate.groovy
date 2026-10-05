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

suite("test_with_false_predicate") {
    def tableName = "false_predicate_table"

    sql """
        DROP TABLE IF EXISTS $tableName 
    """

    sql """
        CREATE TABLE IF NOT EXISTS `$tableName` (
          `k1` int NOT NULL,
          `k2` int NOT NULL,
          `k3` int NOT NULL,
          `v1` int NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`, `k3`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    """

    sql """
        INSERT INTO $tableName VALUES (1, 1, 1, 1);
    """
    sql """
        INSERT INTO $tableName VALUES (2, 2, 2, 2);
    """
    sql """
        INSERT INTO $tableName VALUES (3, 3, 3, 3);
    """

    sql " sync "

    qt_sql """
        select * from $tableName order by k1;
    """

    qt_sql """
        select k1, k2 from $tableName where BITMAP_EMPTY() is  NULL order by k1;
    """

    sql " DROP TABLE $tableName "

}
