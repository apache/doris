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
            }
        }
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}