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

suite("test_clear_block") { 

    // load data
    def load_data = { loadTableName, fileName ->
        streamLoad {
            table loadTableName
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            file fileName
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    sql """ set enable_match_without_inverted_index = false; """
    sql """ set enable_common_expr_pushdown = true """
    // sql """ set 
    def dupTableName = "dup_httplogs"
    sql """ drop table if exists ${dupTableName} """
    // create table
    sql """
        CREATE TABLE IF NOT EXISTS dup_httplogs
        (   
            `id`          bigint NOT NULL AUTO_INCREMENT(100),
            `@timestamp` int(11) NULL,
            `clientip`   varchar(20) NULL,
            `request`    text NULL,
            `status`     int(11) NULL,
            `size`       int(11) NULL,
            INDEX        clientip_idx (`clientip`) USING INVERTED COMMENT '',
            INDEX        request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
            INDEX        status_idx (`status`) USING INVERTED COMMENT '',
            INDEX        size_idx (`size`) USING INVERTED COMMENT ''
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH (`id`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "compaction_policy" = "time_series",
        "inverted_index_storage_format" = "v2",
        "compression" = "ZSTD",
        "disable_auto_compaction" = "true"
        );
    """

    load_data.call(dupTableName, 'documents-1000.json');
    load_data.call(dupTableName, 'documents-1000.json');
    load_data.call(dupTableName, 'documents-1000.json');
    load_data.call(dupTableName, 'documents-1000.json');
    load_data.call(dupTableName, 'documents-1000.json');
    sql """ delete from dup_httplogs where clientip = '40.135.0.0'; """
    sql """ delete from dup_httplogs where status = 304; """
    sql """ delete from dup_httplogs where size = 24736; """
    sql """ delete from dup_httplogs where request = 'GET /images/hm_bg.jpg HTTP/1.0'; """

    sql """ set enable_match_without_inverted_index = false """
    sql """ sync """

    qt_sql """ SELECT clientip from ${dupTableName} WHERE clientip NOT IN (NULL, '') or clientip IN ('17.0.0.0') ORDER BY id LIMIT 2 """
        
    def result1 = sql """ SELECT clientip from ${dupTableName} WHERE clientip NOT IN (NULL, '') or clientip IN ('17.0.0.0') ORDER BY id LIMIT 5000 """
    def result2 = sql """ SELECT clientip from ${dupTableName} WHERE clientip NOT IN (NULL, '') or clientip IN ('17.0.0.0') ORDER BY id LIMIT 5000 """
    if (result1 != result2) {
        logger.info("result1 is: {}", result1)
        logger.info("result2 is: {}", result2)
        assertTrue(false)
    }
    
}