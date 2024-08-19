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

suite("test_select", "arrow_flight_sql") {
    def tableName = "test_select"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        create table ${tableName} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """INSERT INTO ${tableName} VALUES(111, "plsql111")"""
    sql """INSERT INTO ${tableName} VALUES(222, "plsql222")"""
    sql """INSERT INTO ${tableName} VALUES(333, "plsql333")"""
    sql """INSERT INTO ${tableName} VALUES(111, "plsql333")"""
    
    qt_arrow_flight_sql "select sum(id) as a, count(1) as b from ${tableName}"
}
