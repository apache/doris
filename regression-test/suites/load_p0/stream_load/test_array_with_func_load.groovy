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

suite("test_array_with_func_load", "p0") {
    def tableName = "test_array_split_string"
	sql """ set enable_fallback_to_original_planner=false;"""
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
                id int,
                v1 array<VARCHAR(65533)>
                ) ENGINE=OLAP
                duplicate key (`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
                );
    """

    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', """ id,tmp,v1=split_by_string(tmp,'|') """
        file 'test_array_split_string.csv'
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(1, json.NumberLoadedRows)
            }
    }

    sql """sync"""

    qt_sql """select * from ${tableName} order by id;"""

}
