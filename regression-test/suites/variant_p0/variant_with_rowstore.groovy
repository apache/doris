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

suite("regression_test_variant_rowstore", "variant_type"){
    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }
 
    def table_name = "var_rowstore"
    sql "DROP TABLE IF EXISTS ${table_name}"

    sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "false", "store_row_column" = "true");
        """
    sql "set experimental_enable_nereids_planner = false"
    sql "sync"
    sql """insert into ${table_name} values (-3, '{"a" : 1, "b" : 1.5, "c" : [1, 2, 3]}')"""
    sql """insert into  ${table_name} select -2, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : "null", "e" : 7.111}}'  as json_str
            union  all select -1, '{"a": 1123}' as json_str union all select *, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
    sql "sync"
    qt_sql "select * from ${table_name} order by k limit 10"


    table_name = "multi_var_rs"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant,
                v1 variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "false", "store_row_column" = "true");
    """
    sql """insert into ${table_name} select k, cast(v as string), cast(v as string) from var_rowstore"""
    qt_sql "select * from ${table_name} order by k limit 10"

    // Parse url
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def realDb = "regression_test_variant_p0"
    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://locahost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://locahost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    // set server side prepared statement url
    def prepare_url = "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + realDb + "?&useServerPrepStmts=true"
    table_name = "var_rs_pq"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant,
                v1 variant
            )
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "false", "store_row_column" = "true", "enable_unique_key_merge_on_write" = "true");
    """
    sql """insert into ${table_name} select k, cast(v as string), cast(v as string) from var_rowstore"""
    def result1 = connect(user, password, prepare_url) {
        def stmt = prepareStatement "select * from var_rs_pq where k = ?"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement);
        stmt.setInt(1, -3)
        qe_point_select stmt
        stmt.setInt(1, -2)
        qe_point_select stmt
        stmt.setInt(1, -1)
        qe_point_select stmt

        // def stmt1 = prepareStatement "select var['a'] from var_rs_pq where k = ?"
        // assertEquals(stmt1.class, com.mysql.cj.jdbc.ServerPreparedStatement);
        // stmt.setInt(1, -3)
        // qe_point_select stmt
    }

    sql "DROP TABLE IF EXISTS table_rs_invalid_json"
    sql """
        CREATE TABLE table_rs_invalid_json
        (
            col0 BIGINT  NOT NULL,
            coljson VARIANT NOT NULL, INDEX colvariant_idx(coljson) USING INVERTED
        )
        UNIQUE KEY(col0)
        DISTRIBUTED BY HASH(col0) BUCKETS 4
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "store_row_column"="true",
            "replication_num" = "1",
            "inverted_index_storage_format"= "v2"
        );
    """
    sql """insert into table_rs_invalid_json values (1, '1|[""]')"""
    qt_sql "select * from table_rs_invalid_json where col0 = 1"
}