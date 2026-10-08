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
    sql "SET enable_sql_cache = false"

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

    sql "DROP TABLE IF EXISTS uuid_prepared_point_query"
    sql """CREATE TABLE uuid_prepared_point_query (k UUID NOT NULL, v INT)
           UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 3
           PROPERTIES('replication_num'='1','enable_unique_key_merge_on_write'='true','store_row_column'='true')"""
    sql """INSERT INTO uuid_prepared_point_query VALUES
           ('00000000-0000-0000-0000-000000000000',101),
           ('00000000-0000-0000-0000-000000000001',202),
           ('00112233-4455-6677-8899-aabbccddeeff',303),
           ('7fffffff-ffff-ffff-ffff-ffffffffffff',404),
           ('80000000-0000-0000-0000-000000000000',505),
           ('ffffffff-ffff-ffff-ffff-ffffffffffff',606)"""
    qt_prepared_rows "SELECT k,v FROM uuid_prepared_point_query ORDER BY k"

    String database = sql("SELECT DATABASE()")[0][0]
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, database)
    for (boolean enabled : [false, true]) {
        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
            sql "SET enable_short_circuit_query = ${enabled}"
            sql "SET enable_sql_cache = false"
            explain {
                sql """SELECT k,v FROM uuid_prepared_point_query
                       WHERE k = '00000000-0000-0000-0000-000000000001' ORDER BY k"""
                if (enabled) {
                    contains "SHORT-CIRCUIT"
                } else {
                    notContains "SHORT-CIRCUIT"
                }
            }
            for (String predicate : ["k = ?", "? = k"]) {
                def stmt = prepareStatement "SELECT k,v FROM uuid_prepared_point_query WHERE ${predicate} ORDER BY k"
                try {
                    assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)
                    for (String value : ["00000000-0000-0000-0000-000000000000",
                                         "00000000-0000-0000-0000-000000000001",
                                         "00112233445566778899AABBCCDDEEFF",
                                         "00112233-4455-6677-8899-AABBCCDDEEFF",
                                         "7fffffff-ffff-ffff-ffff-ffffffffffff",
                                         "80000000-0000-0000-0000-000000000000",
                                         "ffffffff-ffff-ffff-ffff-ffffffffffff",
                                         "00000000-0000-0000-0000-000000000002",
                                         "00000000-0000-0000-0000-000000000001",
                                         "00000000-0000-0000-0000-000000000000"]) {
                        stmt.setString(1, value)
                        qe_prepared_point stmt
                    }
                } finally {
                    stmt.close()
                }
            }
        }
    }
}
