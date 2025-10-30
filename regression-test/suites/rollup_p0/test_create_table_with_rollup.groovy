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
suite("test_create_table_with_rollup") {

    def tbName = "test_create_table_with_rollup"

    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName}( 
                user_id VARCHAR(32) NOT NULL,
                event_time DATETIME NOT NULL,
                val1 BIGINT NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(user_id, event_time)
            DISTRIBUTED BY HASH(user_id) BUCKETS 1
            ROLLUP(r1(event_time, user_id))
            PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "function_column.sequence_col" = "event_time"
            );
        """

    sql "insert into ${tbName} values(1,'2025-01-01 00:00:00',1)"

    qt_sql "select * from ${tbName}"

    sql "DROP TABLE ${tbName} FORCE;"
}
