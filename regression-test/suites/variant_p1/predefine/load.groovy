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

suite("test_predefine_type_multi_index", "p1"){
    
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    
    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            set 'max_filter_ratio', '0.1'
            set 'memtable_on_sink_node', 'true'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                // assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def table_name = "github_events"
    sql """DROP TABLE IF EXISTS ${table_name}"""
    table_name = "github_events"
    // int rand_subcolumns_count = Math.floor(Math.random() * (611 - 511 + 1)) + 400
    // int rand_subcolumns_count = 0;
    sql "set enable_variant_flatten_nested = true"
    sql """ drop table if exists github_events_2 """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant<
                MATCH_NAME 'repo.name' : string,
                MATCH_NAME 'payload.pull_request.additions' : int,
                MATCH_NAME 'actor.login' : string,
                MATCH_NAME 'type' : string,
                MATCH_NAME 'payload.action' : string,
                MATCH_NAME 'created_at' : datetime,
                MATCH_NAME 'payload.issue.number' : int,
                MATCH_NAME 'payload.comment.body' : string,
                MATCH_NAME 'type.name' : string
            > NULL,
            INDEX idx_var (`v`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true"),
            INDEX idx_var_2 (`v`) USING INVERTED
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4 
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "variant_enable_flatten_nested" = "true");
    """

    // 2015
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-1.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-2.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-3.json'}""")

    // 2022
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-16.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-10.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-22.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-23.json'}""")

    sql """set enable_match_without_inverted_index = false"""
    sql """ set enable_common_expr_pushdown = true """

    qt_sql """select cast(v["repo"]["name"] as string) from github_events where v["repo"]["name"] match 'apache' order by k limit 10;"""
    qt_sql """select cast(v["repo"]["name"] as string) from github_events where cast(v["repo"]["name"] as string) match 'xpressengine/xe-core' order by 1 limit 10;"""
    qt_sql """select cast(v["repo"]["name"] as string) from github_events where cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1 limit 10"""
    
    sql """ drop table if exists github_events_2 """
    sql """ create table github_events_2 like github_events """
    sql """ insert into github_events_2 select * from github_events """

    trigger_and_wait_compaction(table_name, "cumulative")
    qt_sql """select cast(v["repo"]["name"] as string) from github_events where v["repo"]["name"] match 'apache' order by 1 limit 10;"""
    qt_sql """select cast(v["repo"]["name"] as string) from github_events where cast(v["repo"]["name"] as string) match 'xpressengine/xe-core' order by 1 limit 10;"""
    qt_sql """select cast(v["repo"]["name"] as string) from github_events where cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1 limit 10"""
   
    qt_sql """select cast(v["repo"]["name"] as string) from github_events_2 where v["repo"]["name"] match 'apache' order by 1 limit 10;"""
    qt_sql """select cast(v["repo"]["name"] as string) from github_events_2 where cast(v["repo"]["name"] as string) match 'xpressengine/xe-core' order by 1 limit 10;"""
    qt_sql """select cast(v["repo"]["name"] as string) from github_events_2 where cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1 limit 10"""
}
