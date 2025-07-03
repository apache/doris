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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_json_predict_is_null", "p0") {
    sql "DROP TABLE IF EXISTS j_pred"

    sql """
        CREATE TABLE IF NOT EXISTS j_pred (
            id INT,
            j JSON
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
        """

    // insert into valid json rows
    sql """INSERT INTO j_pred VALUES(26, NULL)"""
    sql """INSERT INTO j_pred VALUES(27, '{"k1":"v1", "k2": 200}')"""
    sql """INSERT INTO j_pred VALUES(28, '{"a.b.c":{"k1.a1":"v31", "k2": 300},"a":"niu"}')"""
    // int64 value
    sql """INSERT INTO j_pred VALUES(29, '12524337771678448270')"""
    // int64 min value
    sql """INSERT INTO j_pred VALUES(30, '-9223372036854775808')"""
    // int64 max value
    sql """INSERT INTO j_pred VALUES(31, '18446744073709551615')"""

    // load the jsonb data from csv file
    streamLoad {
        table "j_pred"

        file "test_json.csv" // import csv file
        set 'max_filter_ratio', '0.3'
        time 10000 // limit inflight 10s
        set 'strict_mode', 'true'

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)

            def (code, out, err) = curl("GET", json.ErrorURL)
            log.info("error result: " + out)

            assertEquals("success", json.Status.toLowerCase())
            assertEquals(25, json.NumberTotalRows)
            assertEquals(18, json.NumberLoadedRows)
            assertEquals(7, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    qt_select_extract "SELECT id, j, jsonb_extract(j, '\$.k1') FROM j_pred ORDER BY id"

    qt_select_pred "select * from j_pred where j is null order by id"

    qt_select_delete "delete from j_pred where j is null"

    qt_select_pred "select * from j_pred order by id"

    qt_select_drop "alter table j_pred DROP COLUMN j"

    qt_select "select * from j_pred order by id"
}
