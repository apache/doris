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

suite ("load_json_parse_failed") {

    sql """ DROP TABLE IF EXISTS test; """

    sql """
        CREATE TABLE `test` (
            `event_id` varchar(50) NULL,
            `time_stamp` varchar(50) NULL,
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`event_id`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); 
        """
    streamLoad {
        table "test"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        file './test'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
        }
    }
}
