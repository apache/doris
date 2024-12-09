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

suite("regression_test_variant_mtmv"){
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_materialized_view_nest_rewrite = true"
    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
            set 'jsonpaths', '[\"$.v.id\", \"$.v.type\", \"$.v.actor\", \"$.v.repo\", \"$.v.payload\", \"$.v.public\", \"$.v.created_at\"]'
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

    def table_name = "github_events_mtmv"
    sql """DROP TABLE IF EXISTS ${table_name}"""
    sql """
        CREATE TABLE `${table_name}` (
          `id` BIGINT NOT NULL,
          `type` VARCHAR(30) NULL,
          `actor` VARIANT NULL,
          `repo` VARIANT NULL,
          `payload` VARIANT NULL,
          `public` BOOLEAN NULL,
          `created_at` DATETIME NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );  
    """
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-3.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-16.json'}""")
    sql """DROP MATERIALIZED VIEW IF EXISTS mv1"""
    sql """
        CREATE MATERIALIZED VIEW mv1
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
            as SELECT id, type, actor['id'], actor['display_login'], actor, payload['ref'] FROM github_events_mtmv limit 1024;
    """ 
    String db = context.config.getDbNameByFile(context.file)
    def job_name_1 = getJobName(db, "mv1") 
    waitingMTMVTaskFinished(job_name_1)

    sql """DROP MATERIALIZED VIEW IF EXISTS mv2"""
    sql """
        CREATE MATERIALIZED VIEW mv2
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
            as SELECT id, cast(actor['id'] as bigint) FROM github_events_mtmv limit 1024;
    """ 
    def job_name_2 = getJobName(db, "mv2") 
    waitingMTMVTaskFinished(job_name_2)

    sql """DROP TABLE IF EXISTS tbl1"""
    sql """
        CREATE TABLE tbl1 ( pk int, var VARIANT NULL ) engine=olap DUPLICATE KEY(pk) distributed by hash(pk) buckets 10 properties("replication_num"
= "1");
    """
    sql """insert into tbl1 values (1, '{"a":1}')"""
    sql """DROP MATERIALIZED VIEW IF EXISTS tbl1_mv"""
    sql """
        CREATE MATERIALIZED VIEW tbl1_mv BUILD IMMEDIATE REFRESH AUTO ON MANUAL DISTRIBUTED BY RANDOM BUCKETS 10 PROPERTIES
('replication_num' = '1') AS SELECT * FROM tbl1;
    """
}