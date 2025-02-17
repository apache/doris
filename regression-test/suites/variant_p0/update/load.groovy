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

suite("update_test_load", "p0") {

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

    def table_name = "test_update"

    sql "DROP TABLE IF EXISTS ${table_name}"
    sql "DROP MATERIALIZED VIEW IF EXISTS regression_test_variant_p0_update.table_mv2;"
    sql "DROP MATERIALIZED VIEW IF EXISTS regression_test_variant_p0_update.table_mv4;"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 6
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    for (int i = 0; i < 10; i++) {
        load_json_data.call(table_name, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
    }

    qt_sql """ select count() from ${table_name} """

    createMV ("create materialized view table_mv1 as select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name};")

    explain {
        sql("select max(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv;")
        contains("table_mv1 chose")
    }
    explain {
        sql("select min(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv;")
        contains("table_mv1 chose")
    }
    explain {
        sql("select count(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv;")
        contains("table_mv1 chose")
    }

    qt_sql """ select max(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv; """
    qt_sql """ select min(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv; """
    qt_sql """ select count(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv; """

    sql """
        CREATE MATERIALIZED VIEW table_mv2 BUILD IMMEDIATE REFRESH AUTO ON MANUAL DISTRIBUTED BY RANDOM BUCKETS 6 PROPERTIES
('replication_num' = '1') AS SELECT cast(v['type'] as text), cast(v['public'] as int)  FROM ${table_name};
    """
    waitingMTMVTaskFinishedByMvName("table_mv2")

    explain {
        sql("SELECT sum(cast(v['public'] as int)) FROM ${table_name} group by cast(v['type'] as text) order by cast(v['type'] as text);")
        contains("table_mv2 chose")
    }

    qt_sql """ SELECT sum(cast(v['public'] as int))  FROM ${table_name} group by cast(v['type'] as text) order by cast(v['type'] as text); """


    def create_table_load_data = {create_table_name->
        sql "DROP TABLE IF EXISTS ${create_table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${create_table_name} (
                k bigint,
                v variant NOT NULL
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 6
            properties("replication_num" = "1", "disable_auto_compaction" = "true");
        """

        for (int i = 0; i < 10; i++) {
            load_json_data.call(create_table_name, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
        }
    }

    create_table_load_data.call("test_update_sc")
    create_table_load_data.call("test_update_compact")
    create_table_load_data.call("test_update_sc2")
}
