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
suite("test_update", "p0") {
    def tbName = "test_update"
    for (def use_nereids_planner : [false, true]) {
        sql " SET enable_nereids_planner = $use_nereids_planner; "
        sql " SET enable_fallback_to_original_planner = false; "
        sql "DROP TABLE IF EXISTS ${tbName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                user_id bigint,
                date1 date,
                group_id bigint
            )
            UNIQUE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 5 properties(
                "function_column.sequence_col"='group_id',
                "replication_num" = "1",
                "in_memory"="false"
            );
        """
        sql "insert into ${tbName} values(1,'20240131',100);"
        sql "insert into ${tbName} values(2,'20240131',100);"
        qt_sql1 "select * from ${tbName} order by user_id;"
        // set group_id to 200 on all record
        sql "UPDATE ${tbName} SET group_id=200;"
        // this insert will not work
        sql "insert into ${tbName} values(2,'20240131',100);"
        qt_sql2 "select * from ${tbName} order by user_id;"
        // this insert will work
        sql "insert into ${tbName} values(2,'20240131',300);"
        qt_sql3 "select * from ${tbName} order by user_id;"
        // set group_id to 400 on specific record
        sql "UPDATE ${tbName} SET group_id=400 WHERE user_id=1;"
        qt_sql4 "select * from ${tbName} order by user_id;"
        // this insert will not work
        sql "insert into ${tbName} values(1,'20240131',300);"
        qt_sql5 "select * from ${tbName} order by user_id;"
        // this insert will work
        sql "insert into ${tbName} values(1,'20240131',500);"
        qt_sql6 "select * from ${tbName} order by user_id;"
    }
}
