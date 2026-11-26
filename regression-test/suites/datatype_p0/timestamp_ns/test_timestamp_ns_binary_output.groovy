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

import com.mysql.cj.jdbc.ServerPreparedStatement

import java.sql.PreparedStatement
import java.sql.Types

suite("test_timestamp_ns_binary_output") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword

    sql "drop table if exists test_timestamp_ns_binary_output"
    sql """
        create table test_timestamp_ns_binary_output (
            id int,
            dt timestamp_ns
        )
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_timestamp_ns_binary_output values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1969-12-31 23:59:59.999999999'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '1970-01-01 00:00:00.000000001'),
        (5, '2262-04-11 23:47:16.854775807'),
        (6, null)
    """

    order_qt_text_protocol """
        select id, dt from test_timestamp_ns_binary_output order by id
    """

    String url = getServerPrepareJdbcUrl(
            context.config.jdbcUrl, context.dbName)
    connect(user, password, url) {
        PreparedStatement stmt = prepareStatement("""
            select id, dt from test_timestamp_ns_binary_output where id >= ? order by id
        """)
        assertEquals(ServerPreparedStatement, stmt.class)
        assertEquals(Types.CHAR, stmt.metaData.getColumnType(2))
        stmt.setInt(1, 1)
        qe_binary_protocol stmt
        stmt.close()
    }
}
