package show
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


suite("test_nereids_show_table_creation") {
    def table = "test_nereids_show_table_creation"
    // create table and insert data
    String dbName = 'test_nereids_show_table_creation'
    try_sql """drop database if exists ${dbName}"""
    sql """create database ${dbName}"""
    sql """ drop table if exists ${dbName}.${table} force"""

    sql """
    create table ${dbName}.${table} (
        `id` int(11),
        `name` varchar(128),
        `da` date
    )
    engine=olap
    duplicate key(id)
    partition by range(da)(
        PARTITION p3 VALUES LESS THAN ('2023-01-01'),
        PARTITION p4 VALUES LESS THAN ('2024-01-01'),
        PARTITION p5 VALUES LESS THAN ('2025-01-01')
    )
    distributed by hash(id) buckets 2
    properties(
        "replication_num"="1",
        "light_schema_change"="true"
    );
    """

    checkNereidsExecute("SHOW TABLE CREATION FROM ${dbName}")
    checkNereidsExecute("SHOW TABLE CREATION IN ${dbName}")
    checkNereidsExecute("SHOW TABLE CREATION FROM ${dbName} like '%${table}%'")
    checkNereidsExecute("SHOW TABLE CREATION like '%${table}%'")
    checkNereidsExecute("SHOW TABLE CREATION")

    def res = sql """SHOW TABLE CREATION FROM ${dbName}"""
    assertTrue(res.size() == 0)

    res = sql """SHOW TABLE CREATION IN ${dbName}"""
    assertTrue(res.size() == 0)

    res = sql """SHOW TABLE CREATION FROM ${dbName} like '%${table}%'"""
    assertTrue(res.size() == 0)

    res = sql """SHOW TABLE CREATION like '%${table}%'"""
    assertTrue(res.size() == 0)

    res = sql """SHOW TABLE CREATION"""
    assertTrue(res.size() == 0)

}

