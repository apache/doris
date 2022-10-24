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

suite("test_from_unixtime") {
    def tableName = "test_from_unixtime"

    def timestamp = 1666579511

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs",
            dt_0 DATETIME,
            dt_1 DATETIMEV2
        )
        COMMENT "test from_unixtime table"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """

    // vectorized
    sql """ set enable_vectorized_engine = true; """

    // test stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', """ id, name, dt_0 = from_unixtime(${timestamp}), dt_1 = from_unixtime(${timestamp}) """
        set 'timezone', '+00:00' // specify the time zone used for this load

        file 'test_from_unixtime_streamload.csv'

        time 10000 // limit inflight 10s
    }
    qt_stream_load """ select count(*) from ${tableName} where dt_0 = from_unixtime(${timestamp} - 8*3600); """
    qt_stream_load """ select count(*) from ${tableName} where dt_1 = from_unixtime(${timestamp} - 8*3600); """
 }
