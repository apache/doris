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

suite("test_time_add_sub", "query") {
    def tableName = "test_time_add_sub_function"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                test_time datetime NULL COMMENT ""
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
    sql """ insert into ${tableName} values ("2019-08-01 13:21:03") """
    //years_add 
    qt_sql """ select years_add(test_time,1) result from ${tableName}; """
    //months_add 
    qt_sql """ select months_add(test_time,1) result from ${tableName}; """
    //weeks_add 
    qt_sql """ select weeks_add(test_time,1) result from ${tableName}; """
    //days_add 
    qt_sql """ select days_add(test_time,1) result from ${tableName}; """
    //hours_add 
    qt_sql """ select hours_add(test_time,1) result from ${tableName}; """
    //minutes_add 
    qt_sql """ select minutes_add(test_time,1) result from ${tableName}; """
    //seconds_add 
    qt_sql """ select seconds_add(test_time,1) result from ${tableName}; """

    //years_sub 
    qt_sql """ select years_sub(test_time,1) result from ${tableName}; """
    //months_sub 
    qt_sql """ select months_sub(test_time,1) result from ${tableName}; """
    //weeks_sub 
    qt_sql """ select weeks_sub(test_time,1) result from ${tableName}; """
    //days_sub 
    qt_sql """ select days_sub(test_time,1) result from ${tableName}; """
    //hours_sub 
    qt_sql """ select hours_sub(test_time,1) result from ${tableName}; """
    //minutes_sub 
    qt_sql """ select minutes_sub(test_time,1) result from ${tableName}; """
    //seconds_sub 
    qt_sql """ select seconds_sub(test_time,1) result from ${tableName}; """
}