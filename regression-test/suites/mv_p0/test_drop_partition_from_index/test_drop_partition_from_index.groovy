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

// this suite is for creating table with timestamp datatype in defferent 
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("sql_drop_partition_from_index") {
    def testDb = "test_db"
    def testTable = "test_table"
    def testMv = "test_mv"

    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"

    try {
    sql """DROP DATABASE IF EXISTS ${testDb}"""
    sql """CREATE DATABASE IF NOT EXISTS ${testDb}"""
    sql """USE ${testDb}"""
    sql """
        create table ${testTable} (
                `k1` int not null,
                `k2` int not null,
                `k3` int not null
        )
        engine=olap
        duplicate key(k1, k2, k3)
        partition by list(k1) (
	partition p1 values in ("1","2","3")
        )
        distributed by hash(k1) buckets 1
        properties(
                "replication_num"="1",
                "light_schema_change"="true",
                "compression"="zstd"
        );
       """
   sql"""
       INSERT INTO ${testTable} PARTITION(p1) VALUES(1,1,1),(2,2,2),(3,3,3)
       """
     createMV ("create materialized view ${testMv} as select k1 as a1,k2+k3 from ${testTable}")

     qt_select """ SELECT k1,k2+k3 FROM ${testTable} PARTITION(p1) """
     // index is empty
     def errorSqlResult = """ ALTER TABLE ${testTable} DROP PARTITION p1 FROM INDEX """
     assertTrue(errorSqlResult != null)

     sql""" ALTER TABLE ${testTable} DROP PARTITION p1 FROM INDEX ${testTable} """
     qt_select """ SELECT k1, k2+k3 FROM ${testTable} PARTITION(p1) """
    } finally {
     sql """ DROP MATERIALIZED VIEW IF EXISTS ${testMv} ON ${testTable} """
     sql """ DROP TABLE IF EXISTS ${testTable} """
     sql """ DROP DATABASE ${testDb} """
    }
}

