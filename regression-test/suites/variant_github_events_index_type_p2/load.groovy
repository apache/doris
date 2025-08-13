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

suite("test_variant_index_type_p2", "p2"){
    // prepare test table
    def timeout = 300000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def seed = Math.random()
    def enable_typed_paths_to_sparse = true
    if (seed < 0.5) {
        sql """ set default_variant_enable_typed_paths_to_sparse = true """
        logger.info("enable_typed_paths_to_sparse: true")
        enable_typed_paths_to_sparse = true
    } else {
        sql """ set default_variant_enable_typed_paths_to_sparse = false """
        logger.info("enable_typed_paths_to_sparse: false")
        enable_typed_paths_to_sparse = false
    }
    sql "set enable_variant_flatten_nested = true"
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

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
    int rand_subcolumns_count = Math.floor(Math.random() * (611 - 511 + 1)) + 400
    // int rand_subcolumns_count = 0;
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
                MATCH_NAME 'type.name' : string,
                properties("variant_max_subcolumns_count" = "${rand_subcolumns_count}")
            > NULL,
            INDEX idx_var (`v`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true")
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

    // // // 2022
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-16.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-10.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-22.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-23.json'}""")

    // // // add bloom filter at the end of loading data

    // def tablets = sql_return_maparray """ show tablets from github_events; """
    // // trigger compactions for all tablets in github_events
    trigger_and_wait_compaction("github_events", "cumulative")

    sql """set enable_match_without_inverted_index = false"""
    sql """ set enable_common_expr_pushdown = true """
    // filter by bloom filter
    qt_sql """select cast(v["payload"]["pull_request"]["additions"] as int)  from github_events where cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1;"""
    qt_sql """select * from github_events where  cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1 limit 10"""
    sql """select * from github_events order by k limit 10"""
    qt_sql """select count()  from github_events where v["repo"]["name"] match 'xpressengine' """
    qt_sql """select count()  from github_events where v["repo"]["name"] match 'apache';"""

    sql "DROP TABLE IF EXISTS github_events2"
    sql """
     CREATE TABLE IF NOT EXISTS github_events2 (
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
                MATCH_NAME 'type.name' : string,
                properties("variant_max_subcolumns_count" = "${rand_subcolumns_count}")
            > null,
            INDEX idx_repo_name(v) USING INVERTED PROPERTIES("parser" = "english", "field_pattern" = "repo.name") COMMENT ''
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4 
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "variant_enable_flatten_nested" = "true", "bloom_filter_columns" = "v");
        """
    sql """insert into github_events2 select * from github_events order by k"""
    sql """select v['payload']['commits'] from github_events order by k ;"""
    sql """select v['payload']['commits'] from github_events2 order by k ;"""
    qt_sql """select count() from github_events2"""
    // query with inverted index
    if (!enable_typed_paths_to_sparse) {
        sql """ set enable_match_without_inverted_index = false """
    }
    qt_sql """select count()  from github_events2 where v["repo"]["name"] match 'xpressengine' """
    qt_sql """select count()  from github_events2 where v["repo"]["name"] match 'apache';"""


    sql "DROP TABLE IF EXISTS github_events3"
    sql """
     CREATE TABLE IF NOT EXISTS github_events3 (
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
                MATCH_NAME 'type.name' : string,
                properties("variant_max_subcolumns_count" = "${rand_subcolumns_count}")
            > null,
            INDEX idx_repo_name(v) USING INVERTED PROPERTIES("parser" = "english", "field_pattern" = "repo.name") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4 
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "variant_enable_flatten_nested" = "true", "bloom_filter_columns" = "v");
        """
    sql """insert into github_events3 select * from github_events order by k"""
    // query with inverted index
    if (!enable_typed_paths_to_sparse) {
        sql """ set enable_match_without_inverted_index = false """
    }
    qt_sql """select count()  from github_events3 where v["repo"]["name"] match 'xpressengine' """
    qt_sql """select count()  from github_events3 where v["repo"]["name"] match 'apache';"""

    sql """ drop table if exists github_events4 """
    sql """ create table github_events4 like github_events """
    sql """ insert into github_events4 select * from github_events order by k """
    sql """ drop table github_events """
    sql """ alter table github_events4 rename github_events """
    sql """ alter table github_events set ("bloom_filter_columns" = "v"); """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='github_events' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    // specify schema
    // sql "alter table github_events2 modify column v variant<`payload.comment.id`:int,`payload.commits.url`:text,`payload.forkee.has_pages`:tinyint>"
    // load_json_data.call("github_events2", """${getS3Url() + '/regression/gharchive.m/2022-11-07-23.json'}""")
    // qt_sql "select * from github_events2 WHERE 1=1  ORDER BY k DESC LIMIT 10"
    // qt_sql "select v['payload']['commits'] from github_events2 WHERE 1=1  ORDER BY k DESC LIMIT 10"
    // qt_sql "select v['payload']['commits']['url'] from github_events2 WHERE 1=1  ORDER BY k DESC LIMIT 10"
}
