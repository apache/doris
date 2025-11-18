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

import org.apache.doris.regression.util.Http

import org.apache.doris.regression.suite.ClusterOptions

suite("test_topn_broadcast", "docker") {


    def options = new ClusterOptions()

    options.feNum = 1
    options.beNum = 3
    options.msNum = 1
    options.cloudMode = true
    options.feConfigs += ['example_conf_k1=v1', 'example_conf_k2=v2']
    options.beConfigs += ['enable_file_cache=true', 'enable_java_support=false', 'file_cache_enter_disk_resource_limit_mode_percent=99',
                          'file_cache_background_lru_dump_interval_ms=2000', 'file_cache_background_lru_log_replay_interval_ms=500',
                          'disable_auto_compation=true', 'file_cache_enter_need_evict_cache_in_advance_percent=99',
                          'file_cache_background_lru_dump_update_cnt_threshold=0'
                        ]

    docker(options) {
    // define a sql table
    def indexTbName = "test_topn_broadcast"

    sql "set global enable_two_phase_read_opt = true"
    sql " set global enable_common_expr_pushdown = true "
    sql " set global topn_opt_limit_threshold = 1024 "
    sql "DROP TABLE IF EXISTS ${indexTbName}"
    sql """
      CREATE TABLE ${indexTbName} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 3
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """


    def load_httplogs_data = {table_name, label, read_flag, format_flag, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            file context.config.dataPath + "/fault_injection_p0/documents-1000.json"
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
		        if (ignore_failure && expected_succ_rows < 0) { return }
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
            }
        }
    }

    try {
        load_httplogs_data.call(indexTbName, 'test_topn_broadcast1', 'true', 'json')
        load_httplogs_data.call(indexTbName, 'test_topn_broadcast2', 'true', 'json')
        load_httplogs_data.call(indexTbName, 'test_topn_broadcast3', 'true', 'json')
        sql "sync"

        def explain_result = sql """ explain select * from ${indexTbName} order by `@timestamp` limit 512; """
        println explain_result

        sql """ select * from ${indexTbName} order by `@timestamp` limit 512; """

        // TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        List<List<Object>> tabletRows = sql """ show tablets from ${indexTbName}; """
        def tabletIds = tabletRows.collect { row -> row[0].toString() }.unique()
        assertTrue(tabletIds.size() > 0, "table ${indexTbName} should contain at least one tablet")
        // print tabletIds
        println "Tablet IDs: ${tabletIds}"

        List<List<Object>> backendRows = sql """ show backends """
        def bes = backendRows
            .findAll { row -> row[9].toString().equalsIgnoreCase("true") }
            .collect { row ->
                [
                    host    : row[1].toString(),
                    httpPort: row[4].toString().toInteger()
                ]
            }
        assertTrue(!bes.isEmpty(), "no alive backend hosts available for verification")
        def expectedHostCount = bes.size()

        // Collect which backends report each tablet. New requirement:
        // If any tablet appears in more than one backend's tablets_json, fail the test.
        def tabletPresence = [:].withDefault { [] as List<String> }
        bes.each { be ->
            def response = Http.GET("http://${be.host}:${be.httpPort}/tablets_json?limit=all", true)
            assertEquals(0, response.code as Integer)
            def data = response.data
            def beTablets = data.tablets.collect { it.tablet_id as String }
            tabletIds.each { tabletId ->
                if (beTablets.contains(tabletId)) {
                    tabletPresence[tabletId] << be.host
                }
            }
        }

        tabletIds.each { tabletId ->
            def hosts = tabletPresence[tabletId].unique()
            assertFalse(hosts.size() > 1, "tablet ${tabletId} appears on multiple backends: ${hosts}")
        }

    } finally {
    }
    }
}
