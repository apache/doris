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


// Checklist: F06 F15 I03.
suite("test_uuid_point_query", "p0") {

    sql "DROP TABLE IF EXISTS uuid_point_query"
    sql """CREATE TABLE uuid_point_query (u UUID NOT NULL,v UUID)
           UNIQUE KEY(u) DISTRIBUTED BY HASH(u) BUCKETS 3
           PROPERTIES('replication_num'='1','enable_unique_key_merge_on_write'='true','store_row_column'='true')"""
    sql """INSERT INTO uuid_point_query VALUES
           ('00112233445566778899AABBCCDDEEFF','ffffffff-ffff-ffff-ffff-ffffffffffff'),
           ('80000000000000000000000000000000',NULL)"""
    String query = "SELECT * FROM uuid_point_query WHERE u = CAST('00112233445566778899AABBCCDDEEFF' AS UUID)"
    sql "SET enable_short_circuit_query = true"
    explain {
        sql query
        contains "SHORT-CIRCUIT"
    }
    qt_point query
    sql "SET enable_short_circuit_query = false"
    explain {
        sql query
        notContains "SHORT-CIRCUIT"
    }
    qt_scan query

}
