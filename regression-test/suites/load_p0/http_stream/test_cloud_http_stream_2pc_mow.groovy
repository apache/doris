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

suite("test_cloud_http_stream_2pc_mow", "p0") {
    if (isCloudMode()) {
        def tableName = "test_cloud_http_stream_2pc_mow"

        sql """ drop table if exists ${tableName} """

        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          `k1` int(11) NULL,
          `k2` tinyint(4) NULL,
          `k3` smallint(6) NULL,
          `k4` bigint(20) NULL,
          `k5` largeint(40) NULL,
          `k6` float NULL,
          `k7` double NULL,
          `k8` decimal(9, 0) NULL,
          `k9` char(10) NULL,
          `k10` varchar(1024) NULL,
          `k11` text NULL,
          `k12` date NULL,
          `k13` datetime NULL
          ) ENGINE=OLAP
          UNIQUE KEY(`k1`)
          DISTRIBUTED BY HASH(`k1`) BUCKETS 2
          PROPERTIES (
               "enable_unique_key_merge_on_write" = "true"
          );
        """

        streamLoad {
            table "${tableName}"

            set 'version', '1'
            set 'two_phase_commit', 'true'
            set 'sql', """
                    insert into ${db}.${tableName1} select * from http_stream("column_separator"=",", "format"="csv")
                    """
            time 10000
            file 'all_types.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertEquals(0, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                assertTrue(json.Message.contains('http stream 2pc is unsupported for mow table'))
            }
        }
    }

}
