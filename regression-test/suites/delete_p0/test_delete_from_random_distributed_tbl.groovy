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

suite("test_delete_from_random_distributed_tbl") {
    def tableName = "test_delete_from_random_distributed_tbl"

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """ CREATE TABLE ${tableName} (
                `k` INT NULL, 
                `v` BIGINT SUM NULL
            ) ENGINE=OLAP 
            AGGREGATE KEY(`k`) 
            DISTRIBUTED BY RANDOM BUCKETS 4 
            PROPERTIES ("replication_num"="1") 
        """

    sql """ insert into ${tableName} values(1, 10),(2,10),(2,20)"""
    qt_sql """ select * from ${tableName} order by k """

    sql """ delete from ${tableName} where k=1 """
    qt_sql """ select * from ${tableName} order by k """

    sql """ DROP TABLE IF EXISTS ${tableName}; """
}
