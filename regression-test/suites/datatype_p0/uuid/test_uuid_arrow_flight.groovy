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

import java.sql.Types

suite("test_uuid_arrow_flight", "arrow_flight_sql") {
    sql "DROP TABLE IF EXISTS uuid_flight_paths"
    sql """CREATE TABLE uuid_flight_paths (id INT, u UUID, a ARRAY<UUID>)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO uuid_flight_paths VALUES
           (1, '00112233445566778899AABBCCDDEEFF', ['00112233445566778899AABBCCDDEEFF', NULL]),
           (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff', []), (3, NULL, NULL)"""
    qt_arrow_flight_sql_uuid "SELECT id, u FROM uuid_flight_paths ORDER BY id"
    qt_arrow_flight_sql_nested "SELECT id, a FROM uuid_flight_paths ORDER BY id"
    context.getArrowFlightSqlConnection().createStatement().withCloseable { statement ->
        statement.executeQuery("SELECT u FROM ${context.dbName}.uuid_flight_paths ORDER BY id").withCloseable { result ->
            assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1))
            assertTrue(result.next())
            assertTrue(result.getObject(1) instanceof String)
        }
    }
}
