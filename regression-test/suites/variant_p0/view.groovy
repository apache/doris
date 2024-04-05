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

suite("regression_test_variant_view", "var_view") {
    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
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
    sql """DROP TABLE IF EXISTS github_events_view"""
    sql """
        CREATE TABLE IF NOT EXISTS github_events_view (
            k bigint,
            v variant
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    load_json_data.call("github_events_view", """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    sql " drop view if exists v1"
    sql """create view v1 as SELECT cast(v["repo"]["name"] as string) as repo_name, count() AS stars FROM github_events_view WHERE cast(v["type"] as string) = 'WatchEvent' GROUP BY repo_name  ORDER BY stars DESC, repo_name LIMIT 5;"""
    qt_sql """select * from v1"""
    sql " drop view if exists v2"
    sql """create view v2 as SELECT
            cast(v["repo"]["name"] as string) as repo_name,
            count() AS comments,
            count(distinct cast(v["actor"]["login"] as string)) AS authors
            FROM github_events_view 
            WHERE cast(v["type"] as string) = 'CommitCommentEvent'
            GROUP BY repo_name 
            ORDER BY count() DESC, 1, 3
            LIMIT 50
    """
    qt_sql """select repo_name, comments/authors, comments, authors  from v2;"""
    sql "drop view if exists v3"
    sql """create view v3 as SELECT
            count(distinct(cast(v["actor"]["login"] as string))) AS authors
            FROM github_events_view
    """
    qt_sql "select * from v3"

    sql "drop view if exists v4"
    sql """insert into github_events_view values (1, '{"ratio" : 1.1, "repo" : {"name" : "apache/doris", "id" : 12345}}')"""
    sql """create view v4 as SELECT cast(v["repo"]["name"] as string) repo_name,  cast(v["repo"]["id"] as int) as id from github_events_view where v['ratio'] > 0.7"""
    qt_sql """select * from v4"""
}