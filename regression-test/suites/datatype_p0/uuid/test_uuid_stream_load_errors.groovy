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


// Checklist: A06 G03 G12 G14.
suite("test_uuid_stream_load_errors", "p0") {

    sql "DROP TABLE IF EXISTS uuid_stream_load_errors"
    sql """CREATE TABLE uuid_stream_load_errors (id INT,u UUID NOT NULL)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    for (boolean strict : [false,true]) {
        sql "TRUNCATE TABLE uuid_stream_load_errors"
        Map response
        streamLoad {
            table "uuid_stream_load_errors"
            set "format", "json"
            set "read_json_by_line", "true"
            set "strict_mode", "${strict}"
            set "max_filter_ratio", "0.5"
            inputStream new ByteArrayInputStream(('{' + '"id":1,"u":"00112233445566778899AABBCCDDEEFF"}\n'
                    + '{"id":2,"u":"invalid"}\n'
                    + '{"id":3,"u":"ffffffff-ffff-ffff-ffff-ffffffffffff"}\n').getBytes('UTF-8'))
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) { throw exception }
                response = new groovy.json.JsonSlurper().parseText(result)
                if (response.Status != 'Success') {
                    throw new IllegalStateException("UUID load did not commit: ${response}")
                }
            }
        }
        qt_load_counts "SELECT ${response.NumberTotalRows},${response.NumberLoadedRows},${response.NumberFilteredRows}"
        qt_alignment "SELECT id,u FROM uuid_stream_load_errors ORDER BY id"
    }
    Map rejected
    streamLoad {
        table "uuid_stream_load_errors"
        set "format", "json"
        set "read_json_by_line", "true"
        set "strict_mode", "true"
        set "max_filter_ratio", "0"
        inputStream new ByteArrayInputStream(('{"id":4,"u":"80000000000000000000000000000000"}\n'
                + '{"id":5,"u":"invalid"}\n').getBytes('UTF-8'))
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) { throw exception }
            rejected = new groovy.json.JsonSlurper().parseText(result)
        }
    }
    qt_rejected "SELECT '${rejected.Status}',${rejected.NumberTotalRows},${rejected.NumberFilteredRows}"
    qt_atomicity "SELECT id,u FROM uuid_stream_load_errors ORDER BY id"

}
