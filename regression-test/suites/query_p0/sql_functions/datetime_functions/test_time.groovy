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

suite("test_time") {
    qt_sql """ select time('2025-1-1 12:12:12') """
    qt_sql """ select time('2025-1-1 12:12:61') """
    qt_sql """ select time('2025-1-1 12:61:12') """
    qt_sql """ select time('2025-1-1 25:12:12') """
    def tableName = "test_time_function"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                test_time datetimev2 NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(test_time)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(test_time) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
            )
        """
    sql """ insert into ${tableName} values 
                ("2019-08-01 13:21:03"),
                ("2019-08-01 13:22:03"),
                ("2019-08-01 14:21:03"),
                (null);
    """
    qt_sql_time "select time(test_time) from ${tableName}";
}