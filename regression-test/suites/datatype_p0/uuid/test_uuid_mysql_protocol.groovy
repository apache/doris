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

suite("test_uuid_mysql_protocol", "p0") {
    sql "DROP TABLE IF EXISTS uuid_client_paths"
    sql """CREATE TABLE uuid_client_paths (id INT, u UUID, a ARRAY<UUID>)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO uuid_client_paths VALUES
           (1, '00112233445566778899AABBCCDDEEFF', ['00112233445566778899AABBCCDDEEFF', NULL]),
           (2, 'ffffffff-ffff-ffff-ffff-ffffffffffff', []), (3, NULL, NULL)"""
    qt_text "SELECT * FROM uuid_client_paths ORDER BY id"
    String database = sql("SELECT DATABASE()")[0][0]
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, database)
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        def stmt = prepareStatement "SELECT * FROM uuid_client_paths WHERE u <=> CAST(? AS UUID) ORDER BY id"
        try {
            assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt.class)
            for (String value : ["00112233445566778899AABBCCDDEEFF",
                                 "ffffffff-ffff-ffff-ffff-ffffffffffff", null,
                                 "00112233-4455-6677-8899-aabbccddeeff"]) {
                if (value == null) {
                    stmt.setNull(1, Types.VARCHAR)
                } else {
                    stmt.setString(1, value)
                }
                qe_binary stmt
                def metadata = stmt.getMetaData()
                assertEquals(Types.CHAR, metadata.getColumnType(2))
                assertEquals("java.lang.String", metadata.getColumnClassName(2))
            }
        } finally {
            stmt.close()
        }
    }
}
