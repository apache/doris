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

suite("test_uuid_point_query_light_schema_change", "p0") {
    sql "DROP TABLE IF EXISTS uuid_point_query_lsc"
    sql """CREATE TABLE uuid_point_query_lsc (k INT NOT NULL, v INT)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='true',
                   'light_schema_change'='true', 'store_row_column'='true',
                   'row_store_columns'='k')"""
    sql "INSERT INTO uuid_point_query_lsc VALUES (1,10), (2,20)"
    sql "SET enable_short_circuit_query=true"
    sql "SET enable_short_circuit_query_access_column_store=true"

    sql """ALTER TABLE uuid_point_query_lsc ADD COLUMN u_default UUID NOT NULL
        DEFAULT '00112233-4455-6677-8899-aabbccddeeff'"""
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='uuid_point_query_lsc' ORDER BY CreateTime DESC LIMIT 1"
        time 600
    }
    sql "ALTER TABLE uuid_point_query_lsc ADD COLUMN u_null UUID NULL"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='uuid_point_query_lsc' ORDER BY CreateTime DESC LIMIT 1"
        time 600
    }

    // No load has propagated the new schema to BE, and neither UUID is in the row store.
    explain {
        sql "SELECT k,u_default,u_null FROM uuid_point_query_lsc WHERE k=1"
        contains "SHORT-CIRCUIT"
    }
    qt_before_load "SELECT k,u_default,u_null FROM uuid_point_query_lsc WHERE k=1 ORDER BY k"
    qt_missing_key "SELECT k,u_default,u_null FROM uuid_point_query_lsc WHERE k=3 ORDER BY k"

    sql """INSERT INTO uuid_point_query_lsc VALUES
        (3,30,'80000000-0000-0000-0000-000000000000','ffffffff-ffff-ffff-ffff-ffffffffffff')"""
    qt_old_row "SELECT k,u_default,u_null FROM uuid_point_query_lsc WHERE k=2 ORDER BY k"
    qt_new_row "SELECT k,u_default,u_null FROM uuid_point_query_lsc WHERE k=3 ORDER BY k"
}
