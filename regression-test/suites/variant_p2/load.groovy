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

suite("github_event_advance", "variant_type"){

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

    def create_table = {table_name, buckets="auto" ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
            id BIGINT NOT NULL,
            type VARCHAR(30) NULL,
            actor VARIANT NULL,
            repo VARIANT NULL,
            payload VARIANT NULL,
            public BOOLEAN NULL,
            created_at DATETIME NULL,
            org JSON NULL
            -- INDEX idx_payload(payload) USING INVERTED PROPERTIES("parser" = "english") COMMENT '',
            -- INDEX idx_repo(repo) USING INVERTED PROPERTIES("parser" = "english") COMMENT '',
            -- INDEX idx_actor(actor) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS ${buckets}
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }

    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

    try {
        set_be_config.call("ratio_of_defaults_as_sparse_column", "1")
        table_name = "github_events"
        create_table.call(table_name, 10)
        List<Long> daysEveryMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        // 2015
        def year = "2015"
        def monthPrefix = "0"
        def dayPrefix = "0"
        log.info("current year: ${year}")
        for (int i = 1; i <= 3; i++) {
            def month = i < 10 ? monthPrefix + i.toString() : i.toString()
            log.info("current month: ${month}")
            for (int j = 1; j <= daysEveryMonth[i - 1]; j++) {
                def day = j < 10 ? dayPrefix + j.toString() : j.toString()
                log.info("current day: ${day}")
                for (int z = 1; z < 24; z++) {
                    def hour = z.toString()
                    log.info("current hour: ${hour}")
                    def fileName = year + "-" + month + "-" + day + "-" + hour + ".json"
                    log.info("cuurent fileName: ${fileName}")
                    load_json_data.call(table_name, """${getS3Url() + '/regression/github_events_dataset/' + fileName}""")
                }
            }
        }
        
        qt_sql("select count() from github_events")
    } finally {
        // reset flags
        set_be_config.call("ratio_of_defaults_as_sparse_column", "0.95")
    }
}
