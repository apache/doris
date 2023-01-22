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

suite("test_cast_to_datetimev2_function") {

    def tableName = "test_cast_to_datetimev2_function_table"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
    	    CREATE TABLE IF NOT EXISTS ${tableName} (
                c_id INT,
                c_date_timev2 DATETIMEV2(4),
                c_string VARCHAR(30))
            DISTRIBUTED BY HASH(c_id) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            )
            """
        sql "INSERT INTO ${tableName} values(1,'2022-12-01 22:23:24.9999','2022-12-01 22:23:24.999999')"
        sql "INSERT INTO ${tableName} values(2,'2022-12-02 22:23:24.9999','2022-12-02 22:23:24.999999')"
        
        qt_select1 "select * from ${tableName} order by c_id asc"
        qt_select2 "select c_id, c_date_timev2, cast(c_string as datetimev2(4)) from ${tableName} order by c_id asc"

        qt_select3 "select cast ('2022-12-02 22:23:24.999999' as datetimev2(4)),cast ('2022-12-02 22:23:23.999999' as datetimev2(4))"
        qt_select4 "select cast ('2022-12-02 22:23:24.999999' as datetimev2(4)),cast ('2022-12-02 22:23:23.999999' as datetimev2(4)) from ${tableName}"
        sql "DROP TABLE IF EXISTS ${tableName}"
        qt_select5 "select cast ('2022-12-02 22:23:24.999999' as datetimev2(4)),cast ('2022-12-02 22:23:23.999999' as datetimev2(4))"
}