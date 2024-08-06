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

import com.mysql.cj.jdbc.StatementImpl
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_max_scan_key_num", "query,p0") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def feHost = context.getFeHttpAddress().hostString
    def feHttpPort = context.getFeHttpAddress().port
    def curl_prefix = "curl -X GET -u ${user}:${password} http://${feHost}:${feHttpPort}"

    sql "create database if not exists test_query_db"
    sql "use test_query_db"

    def tableName = "test_max_scan_key_num"
    sql "drop table if exists ${tableName}"
    sql """
        CREATE TABLE `${tableName}` (
            `k` INT NULL,
            `V` BIGINT SUM NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
           "replication_num" = "1"
        );
    """

    sql "insert into ${tableName} values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5)"

    def get_query_id = { trace_id ->
        StringBuilder sb = new StringBuilder();
        sb.append(curl_prefix).append("/rest/v2/manager/query/trace_id/").append(trace_id)
        String command = sb.toString()
        def process = command.execute()
        def code = process.waitFor()
        assertEquals(code, 0)
        def out = process.getText()
        def json = parseJson(out)
        return json.data.toString()
    }

    def get_query_profile = { query_id ->
        StringBuilder sb = new StringBuilder();
        sb.append(curl_prefix)
        sb.append("/api/profile?query_id=").append(query_id)
        String command = sb.toString()
        def process = command.execute()
        def code = process.waitFor()
        assertEquals(code, 0)
        def out = process.getText()
        def json = parseJson(out)
        return json.data.toString()
    }

    def trace_id = UUID.randomUUID().toString()
    sql """ set session_context="trace_id:${trace_id}" """
    sql """ set enable_profile = true """
    sql """ set max_scan_key_num = 3 """

    def sql_str = """ select * from ${tableName} where k in (1, 3, 14, 58, 60) """
    qt_sql sql_str

    def query_id = get_query_id trace_id
    def profile_text = get_query_profile query_id
    profile_text = profile_text.replace(" ","")

    assertTrue(profile_text.contains(query_id))
    assertTrue(profile_text.contains("ScanKey=[1:3]ScanKey=[14:14]ScanKey=[58:60]"))
    assertTrue(profile_text.contains("KeyRangesNum:3"))

    sql "drop table ${tableName}"
}