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

import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.text.SimpleDateFormat

suite("test_stream_load_timestamp", "p0") {
    sql "show tables"

    String tableName = "test_stream_load_timestamp_table"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id int, t timestamp)
        ENGINE=OLAP
        UNIQUE KEY(id)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql "set time_zone='+8:00';"
    sql "insert into ${tableName} values(1, '2020-01-01 10:00:00');"
    qt_sql1 "select * from ${tableName};"
    sql "set time_zone='+10:00';"
    sql "insert into ${tableName} values(2, '2020-01-01 10:00:00');"
    qt_sql2 "select * from ${tableName};"

    sql "set time_zone='+8:00';"
    qt_sql3 "select * from ${tableName};"

    sql "truncate table ${tableName}"


    // test +8:00
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'time_zone', '+8:00'
  

        file 'test_stream_load_timestamp.csv'
        time 10000 // limit inflight 10s
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(!result.contains("ErrorURL"))
        }
    }

    sql "sync"
    qt_sql_stream_load1 "select * from ${tableName} order by id"


    sql "truncate table ${tableName}"

    // test +10:00
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'time_zone', '+10:00'
  

        file 'test_stream_load_timestamp.csv'
        time 10000 // limit inflight 10s
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertTrue(!result.contains("ErrorURL"))
        }
    }

    sql "sync"
    qt_sql_stream_load2 "select * from ${tableName} order by id"
}



