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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

suite("load_p2", "variant_type,p2"){
    def env = System.getenv()
    boolean use_stream_load = env
            .getOrDefault("VARIANT_P2_USE_STREAM_LOAD", "false").toBoolean()
    def resumeFiles = env.getOrDefault("VARIANT_P2_RESUME_FILES", "")
            .split(",").findAll { !it.isEmpty() }
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
            timeout 60000
            retryIfHttpError true

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
        sql "set default_variant_max_subcolumns_count = 0"
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

    // Configuration for the number of threads
    def numberOfThreads = 20 // Set this to your desired number of threads

    // Executor service for managing threads
    def executorService = Executors.newFixedThreadPool(numberOfThreads)
    def futures = []

    try {
        def table_name = "github_events"
        def s3load_paral_wait = {tbl, fmt, path, paral ->
            String ak = getS3AK()
            String sk = getS3SK()
            String s3BucketName = getS3BucketName()
            String s3Endpoint = getS3Endpoint()
            String s3Region = getS3Region()
            def load_label = "part_" + UUID.randomUUID().toString().replace("-", "0")
            sql """
                LOAD LABEL ${load_label} (
                    DATA INFILE("s3://${s3BucketName}/${path}")
                    INTO TABLE ${tbl}
                    COLUMNS TERMINATED BY ","
                    FORMAT AS "${fmt}"
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "$ak",
                    "AWS_SECRET_KEY" = "$sk",
                    "AWS_ENDPOINT" = "${s3Endpoint}",
                    "AWS_REGION" = "${s3Region}",
                    "provider" = "${getS3Provider()}"
                )
                PROPERTIES(
                    "load_parallelism" = "${paral}"
                );
            """
            // Waiting for job finished or cancelled
            def max_try_milli_secs = 600000
            while (max_try_milli_secs > 0) {
                String[][] result = sql """ show load where label="$load_label" order by createtime desc limit 1; """
                if (result[0][2].equals("FINISHED")) {
                    logger.info("Load FINISHED " + load_label)
                    break;
                }
                if (result[0][2].equals("CANCELLED")) {
                    log.warn("load failed: $result")
                    assertTrue(false, "load failed: $result")
                    break;
                }
                Thread.sleep(6000)
                max_try_milli_secs -= 6000
                if(max_try_milli_secs <= 0) {
                    assertTrue(1 == 2, "load Timeout: $load_label")
                }
            }
        }
        def submitFile = { fileName ->
            if (use_stream_load) {
                def fileUrl = """${getS3Url() + '/regression/github_events_dataset/' + fileName}"""
                futures << executorService.submit({
                    log.info("Loading file: ${fileName}")
                    load_json_data.call(table_name, fileUrl)
                } as Runnable)
            } else {
                futures << executorService.submit({
                    log.info("Loading file: ${fileName}")
                    s3load_paral_wait.call(table_name, "JSON",
                            "regression/github_events_dataset/${fileName}", 3)
                } as Runnable)
            }
        }
        if (resumeFiles) {
            assertTrue(use_stream_load, "Resume requires VARIANT_P2_USE_STREAM_LOAD=true")
            resumeFiles.each { submitFile.call(it) }
        } else {
            create_table.call(table_name)
            List<Long> daysEveryMonth = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
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
                        def fileName = year + "-" + month + "-" + day + "-" + z + ".json"
                        log.info("current fileName: ${fileName}")
                        submitFile.call(fileName)
                    }
                }
            }
        }
        
        try {
            futures.each { future ->
                future.get()
            }
        } catch (Exception e) {
            throw e.cause // throw original exception
        } finally {
            // Shutdown executor service and wait for all tasks to complete
            executorService.shutdown()
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
        }

        qt_sql("select count() from github_events")
    } finally {
        // reset flags
    }
}
