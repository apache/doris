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

suite("test_stream_load_error_url", "p0") {
    def tableName = "test_stream_load_error_url"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `product_id` BIGINT NULL,
                `market_code` VARCHAR(32),
                `date` DATE NULL,
                `event_type` VARCHAR(255) NULL,
                `new_value` TEXT NULL,
                `old_value` TEXT NULL,
                `release_note` TEXT NULL,
                `release_date` TEXT NULL,
                `u_time` DATETIME NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`product_id`, `market_code`, `date`, `event_type`)
            COMMENT 'test_error_url'
            PARTITION BY RANGE(`date`)
            (PARTITION p_201001 VALUES [('2010-01-01'), ('2012-01-01')),
            PARTITION p_201201 VALUES [('2012-01-01'), ('2014-01-01')),
            PARTITION p_201401 VALUES [('2014-01-01'), ('2016-01-01')),
            PARTITION p_201601 VALUES [('2016-01-01'), ('2018-01-01')),
            PARTITION p_201801 VALUES [('2018-01-01'), ('2020-01-01')),
            PARTITION p_202001 VALUES [('2020-01-01'), ('2022-01-01')),
            PARTITION p_202201 VALUES [('2022-01-01'), ('2024-01-01')),
            PARTITION p_202401 VALUES [('2024-01-01'), ('2026-01-01')),
            PARTITION p_202601 VALUES [('2026-01-01'), ('2028-01-01')),
            PARTITION p_202801 VALUES [('2028-01-01'), ('2028-12-01')))
            DISTRIBUTED BY HASH(`product_id`, `market_code`, `date`, `event_type`) BUCKETS 10
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """

        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'columns', 'k1, k2, k3'
            file 'test_error_url.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
                def (code, out, err) = curl("GET", json.ErrorURL)
                log.info("error result: " + out)
                assertTrue(out.contains("actual column number in csv file is  more than  schema column number.actual number"))
                log.info("url: " + json.ErrorURL)
                if (isCloudMode()) {
                    assertTrue(json.ErrorURL.contains("X-Amz-Signature="))
                }
            }
        }
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `time` DATETIME(6) NULL,
                `__docid` VARCHAR(64) NULL,
                `__source` TEXT NULL COMMENT 'hidden',
                `message` TEXT NULL,
                `__namespace` TEXT NULL COMMENT 'hidden',
                `source` TEXT NULL,
                `service` TEXT NULL,
                `container_host` TEXT NULL,
                `endpoint` TEXT NULL,
                `env` TEXT NULL,
                `http_host` TEXT NULL,
                `http_method` TEXT NULL,
                `http_route` TEXT NULL,
                `http_status_code` TEXT NULL,
                `http_url` TEXT NULL,
                `operation` TEXT NULL,
                `project` TEXT NULL,
                `source_type` TEXT NULL,
                `status` TEXT NULL,
                `span_type` TEXT NULL,
                `parent_id` TEXT NULL,
                `resource` TEXT NULL,
                `span_id` TEXT NULL,
                `trace_id` TEXT NULL,
                `sample_rate` DOUBLE NULL,
                `date` BIGINT NULL,
                `create_time` BIGINT NULL,
                `priority` BIGINT NULL,
                `duration` BIGINT NULL,
                `start` BIGINT NULL,
                `var` TEXT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`time`, `__docid`)
            COMMENT 'default'
            PARTITION BY RANGE(`time`)
            (PARTITION p20240625 VALUES [('2024-06-25 00:00:00'), ('2024-06-26 00:00:00')),
            PARTITION p20240626 VALUES [('2024-06-26 00:00:00'), ('2024-06-27 00:00:00')))
            DISTRIBUTED BY RANDOM BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "DAY",
            "dynamic_partition.time_zone" = "Asia/Shanghai",
            "dynamic_partition.start" = "-100000",
            "dynamic_partition.end" = "1",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.replication_allocation" = "tag.location.default: 1",
            "dynamic_partition.buckets" = "10",
            "dynamic_partition.create_history_partition" = "false",
            "dynamic_partition.history_partition_num" = "16",
            "dynamic_partition.hot_partition_num" = "0",
            "dynamic_partition.reserved_history_periods" = "NULL",
            "dynamic_partition.storage_policy" = "",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728"
            );
        """

        streamLoad {
            table "${tableName}"
            set 'column_separator', '|'
            set 'columns', '`time`,`__docid`,`__source`,`message`,`__namespace`,`source`,`service`,`container_host`,`endpoint`,`env`,`http_host`,`http_method`,`http_route`,`http_status_code`,`http_url`,`operation`,`project`,`source_type`,`status`,`span_type`,`parent_id`,`resource`,`span_id`,`trace_id`,`sample_rate`,`date`,`create_time`,`priority`,`duration`,`start`,`var`'
            file 'test_error_url_1.csv'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
                def (code, out, err) = curl("GET", json.ErrorURL)
                log.info("error result: " + out)
                assertTrue(out.contains("no partition for this tuple"))
                log.info("url: " + json.ErrorURL)
            }
        }
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}