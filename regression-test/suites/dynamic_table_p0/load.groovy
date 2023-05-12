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

suite("regression_test_dynamic_table", "dynamic_table"){
    // prepare test table

    def load_json_data = {table_name, vec_flag, format_flag, read_flag, file_name, expect_success, rand_id=false ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'read_json_by_line', read_flag
            if (rand_id) {
                set 'columns', 'id= rand() * 100000'
            }

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
                if (expect_success == "false") {
                    assertEquals("fail", json.Status.toLowerCase())
                } else {
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    def real_alter_res = "true"
    def res = "null"
    def wait_for_alter_finish = "null"
    def check_time = 30
    def json_load = {src_json, table_name ->
        //create table
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                id bigint,
                ...
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 5 
            properties("replication_num" = "1");
        """

        //stream load src_json
        load_json_data.call(table_name, 'true', 'json', 'true', src_json, 'true')
        sleep(1000)
    }
    def json_load_unique = {src_json, table_name ->
        //create table
        table_name = "${table_name}_unique";
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name}(
                id bigint,
                ...
            )
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 5 
            properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

        //stream load src_json
        load_json_data.call(table_name, 'true', 'json', 'true', src_json, 'true', true)
        sleep(1000)
    }

    def json_load_nested = {src_json, table_name ->
        //create table
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                qid bigint,
		        creationdate datetime,
                `answers.date` array<datetime>,
                `title` string,
		        INDEX creation_date_idx(`creationdate`) USING INVERTED COMMENT 'creationdate index',
 		        INDEX title_idx(`title`) USING INVERTED PROPERTIES("parser"="standard") COMMENT 'title index',
		        ...
            )
            DUPLICATE KEY(`qid`)
            DISTRIBUTED BY RANDOM BUCKETS 5 
            properties("replication_num" = "1");
        """

        //stream load src_json
        load_json_data.call(table_name, 'true', 'json', 'true', src_json, 'true')
        sleep(1000)
    }
    // json_load("btc_transactions.json", "test_btc_json")
    // json_load("ghdata_sample.json", "test_ghdata_json")
    // json_load("nbagames_sample.json", "test_nbagames_json")
    // json_load_nested("es_nested.json", "test_es_nested_json")
    // json_load_unique("btc_transactions.json", "test_btc_json")
    // json_load_unique("ghdata_sample.json", "test_ghdata_json")
    // json_load_unique("nbagames_sample.json", "test_nbagames_json")
    // sql """insert into test_ghdata_json_unique select * from test_ghdata_json"""
    // sql """insert into test_btc_json_unique select * from test_btc_json"""

    // abnormal cases
    table_name = "abnormal_cases" 
    sql """
            DROP TABLE IF EXISTS ${table_name};
    """
    sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                qid bigint,
		        ...
            )
            DUPLICATE KEY(`qid`)
            DISTRIBUTED BY HASH(`qid`) BUCKETS 5 
            properties("replication_num" = "1");
    """
    load_json_data.call(table_name, 'true', 'json', 'true', "invalid_dimension.json", 'false')
    load_json_data.call(table_name, 'true', 'json', 'true', "invalid_format.json", 'false')
    load_json_data.call(table_name, 'true', 'json', 'true', "floating_point.json", 'true')
    load_json_data.call(table_name, 'true', 'json', 'true', "floating_point2.json", 'true')
    load_json_data.call(table_name, 'true', 'json', 'true', "floating_point3.json", 'true')
    load_json_data.call(table_name, 'true', 'json', 'true', "uppercase.json", 'true')

    qt_sql "select * from ${table_name}"

    // load more
    table_name = "gharchive";
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE gharchive (
            created_at datetime NOT NULL COMMENT '',
            id varchar(30) default 'defualt-id' COMMENT '',
            type varchar(50) NULL COMMENT '',
            public boolean NULL COMMENT '',
            ...
        )
        ENGINE=OLAP
        DUPLICATE KEY(created_at)
        DISTRIBUTED BY HASH(id) BUCKETS 32
        PROPERTIES (
            'replication_allocation' = 'tag.location.default: 1'
        ); 
        """
    def paths = [
        """${getS3Url() + '/regression/gharchive/2015-01-01-22.json'}""",
        """${getS3Url() + '/regression/gharchive/2015-01-01-16.json'}""",
        """${getS3Url() + '/regression/gharchive/2016-01-01-16.json'}""",
    ]
    for (String path in paths) {
        streamLoad {
            // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table "${table_name}"
            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file path 
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals('success', json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    sql 'sync'
    meta = sql_meta 'select * from gharchive limit 1'
    def array_cols = [
        "payload.commits.url",
        "payload.commits.sha",
        "payload.commits.author.email",
        "payload.commits.distinct",
        "payload.commits.author.name",
        "payload.commits.message",
        "payload.issue.labels.name",
        "payload.issue.labels.color",
        "payload.issue.labels.url",
        "payload.pages.title",
        "payload.pages.html_url",
        "payload.pages.sha",
        "payload.pages.action",
        "payload.pages.page_name",
        "payload.release.assets.uploader.repos_url",
        "payload.release.assets.uploader.id",
        "payload.release.assets.uploader.organizations_url",
        "payload.release.assets.uploader.received_events_url",
        "payload.release.assets.uploader.site_admin",
        "payload.release.assets.uploader.subscriptions_url",
        "payload.release.assets.state",
        "payload.release.assets.size",
        "payload.release.assets.uploader.following_url",
        "payload.release.assets.uploader.starred_url",
        "payload.release.assets.download_count",
        "payload.release.assets.created_at",
        "payload.release.assets.updated_at",
        "payload.release.assets.browser_download_url",
        "payload.release.assets.url",
        "payload.release.assets.uploader.gravatar_id",
        "payload.release.assets.uploader.gists_url",
        "payload.release.assets.uploader.url",
        "payload.release.assets.content_type",
        "payload.release.assets.name",
        "payload.release.assets.uploader.login",
        "payload.release.assets.uploader.avatar_url",
        "payload.release.assets.uploader.html_url",
        "payload.release.assets.uploader.followers_url",
        "payload.release.assets.uploader.events_url",
        "payload.release.assets.uploader.type",
        "payload.release.assets.id",
        "payload.release.assets.label"
    ]
    for (List<String> col_meta in meta) {
        if (col_meta[0] in array_cols) {
            qt_sql "select sum(array_size(`${col_meta[0]}`)) from gharchive"
        } else {
            qt_sql "select count(`${col_meta[0]}`) from gharchive"
        }
    } 
}
