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

suite("test_delete_nullsafe_eq", "delete_p0") {
    def dbName = "repro_db_delete_nullsafe_eq"
    def tableName = "t_core"

    sql """ DROP DATABASE IF EXISTS ${dbName} """
    sql """ CREATE DATABASE ${dbName} """
    sql """ USE ${dbName} """

    try {
        sql """
            CREATE TABLE ${tableName} (
              k1 INT,
              v1 INT
            )
            DUPLICATE KEY(k1)
            PARTITION BY RANGE(k1) (
              PARTITION p1 VALUES LESS THAN ("100")
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES(
              "replication_num" = "1",
              "disable_auto_compaction" = "true"
            )
        """

        sql """ INSERT INTO ${tableName}(k1, v1) VALUES (1, 10) """
        sql """ INSERT INTO ${tableName}(k1, v1) VALUES (2, 20) """

        sql """ set delete_without_partition = true """
        sql """ DELETE FROM ${tableName} WHERE k1 <=> 1 """
        sql """ sync """

        qt_after_delete """ SELECT * FROM ${tableName} ORDER BY k1 """
    } finally {
        sql """ set delete_without_partition = false """
        sql """ DROP DATABASE IF EXISTS ${dbName} """
    }
}
