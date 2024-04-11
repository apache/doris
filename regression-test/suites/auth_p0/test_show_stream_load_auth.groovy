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

import org.junit.Assert;

suite("test_show_stream_load_auth","p0,auth") {
    String tableName = "test_show_stream_load_auth_table"
    String label = "test_show_stream_load_auth_label" + System.currentTimeMillis();
    String user = 'test_show_stream_load_auth_user'
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """ DROP TABLE IF EXISTS ${tableName} """


    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k1, k2'
        set 'label', label
        set 'strict_mode', 'true'

        file 'test_strict_mode.csv'
        time 10000 // limit inflight 10s
    }

    Thread.sleep(60000);
    def res = sql "SHOW STREAM LOAD from regression_test_auth_p0 where label = '${label}'"
    log.info(res.toString())
    if(res.size() == 0) {
        // `show stream load` has some delay, and need be config `enable_stream_load_record=true`
        // we not sure when can has result, so if `admin` can not get res, ignore this case.
        return;
    }

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
       res = sql "SHOW STREAM LOAD from regression_test_auth_p0 where label = '${label}'"
       log.info(res.toString())
       assertFalse(res.toString().contains("${label}"))
    }
    sql """grant load_priv on regression_test_auth_p0.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
       res = sql "SHOW STREAM LOAD from regression_test_auth_p0 where label = '${label}'"
       log.info(res.toString())
       assertTrue(res.toString().contains("${label}"))
    }
    try_sql("DROP USER ${user}")
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
