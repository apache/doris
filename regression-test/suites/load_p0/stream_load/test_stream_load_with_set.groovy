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

suite("test_stream_load_with_set", "load_p0") {
    def tableName = "stream_load_with_set"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName}
        (
            k00 INT             NOT NULL,
            k01 DATE            NOT NULL,
            k02 BOOLEAN         NULL,
            k03 TINYINT         NULL,
            k04 SMALLINT        NULL,
            k05 INT             NULL,
            k06 BIGINT          NULL,
            k07 LARGEINT        NULL,
            k08 FLOAT           NULL,
            k09 DOUBLE          NULL,
            k10 DECIMAL(9,1)    NULL,
            k11 DECIMALV3(9,1)  NULL,
            k12 DATETIME        NULL,
            k13 DATEV2          NULL,
            k14 DATETIMEV2      NULL,
            k15 CHAR            NULL,
            k16 VARCHAR         NULL,
            k17 STRING          NULL,
            k18 JSON            NULL,
            kd01 DATE           NOT NULL,
            kd02 INT            NULL,
            kd03 VARCHAR(256)   NULL,
            kd04 DATE           NOT NULL
        )
        DUPLICATE KEY(k00)
        DISTRIBUTED BY HASH(k00) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'columns', "k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,kd01=20240123, kd02=abs(-2)+3, kd03=uuid(), kd04=now()"
        file "basic_data.csv"
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(20, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }

    streamLoad {
        table "${tableName}"
        set 'column_separator', '|'
        set 'columns', "kd01=20240123, kd02=abs(-2)+3, kd03=uuid(), kd04=now()"
        file "basic_data.csv"
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(20, json.NumberTotalRows)
            assertEquals(20, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertEquals(0, json.NumberUnselectedRows)
        }
    }
    qt_select """ select count(*) from ${tableName} """
}
