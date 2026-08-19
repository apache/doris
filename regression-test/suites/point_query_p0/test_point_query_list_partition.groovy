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

suite("test_point_query_list_partition") {
    // https://github.com/apache/doris/issues/66030
    // Short-circuit point query on LIST-partitioned table used to fail with
    // IllegalStateException when the partition predicate was pruned as always-true.
    def tableName = "tbl_point_query_list_partition"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
            pk varchar(64),
            _id bigint
        )
        UNIQUE KEY(pk, _id)
        PARTITION BY LIST (`pk`) (
            PARTITION p_abcd VALUES IN ('abcd'),
            PARTITION p_ab VALUES IN ('a', 'b')
        )
        DISTRIBUTED BY HASH(pk, _id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "store_row_column" = "true",
            "light_schema_change" = "true"
        )
    """
    sql """INSERT INTO ${tableName} (pk, _id) VALUES ('abcd', 1), ('a', 2)"""

    explain {
        sql("SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 1")
        contains "SHORT-CIRCUIT"
    }

    // single-value list partition: predicate pk='abcd' is fully implied by the partition
    qt_hit_single_value """SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 1"""
    qt_miss """SELECT * FROM ${tableName} WHERE pk = 'abcd' AND _id = 2"""
    // multi-value list partition: predicate pk='a' is not fully implied
    qt_hit_multi_value """SELECT * FROM ${tableName} WHERE pk = 'a' AND _id = 2"""

    // server-side prepared statement path
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    def prepare_url = "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + context.dbName + "?&useServerPrepStmts=true"

    connect(user, password, prepare_url) {
        def stmt = prepareStatement "select * from ${tableName} where pk = ? and _id = ?"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement);
        stmt.setString(1, 'abcd')
        stmt.setLong(2, 1)
        qe_point_select_prepared_hit stmt
        stmt.setString(1, 'abcd')
        stmt.setLong(2, 2)
        qe_point_select_prepared_miss stmt
        stmt.setString(1, 'a')
        stmt.setLong(2, 2)
        qe_point_select_prepared_multi stmt
    }
}
