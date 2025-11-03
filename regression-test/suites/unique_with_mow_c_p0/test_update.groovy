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

suite("test_update") {
    def tableName = "test_update"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    onFinish {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

    sql """
        CREATE TABLE `$tableName` (
            `a` int NOT NULL,
            `b` int NOT NULL,
            `c` int NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`a`)
        CLUSTER BY (`b`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
         );
    """

    sql """ insert into $tableName values(10, 20, 3), (11, 21, 3), (12, 22, 32); """
    sql """ update $tableName set b = 200 where c = 3; """
    order_qt_sql "select * from $tableName"
}
