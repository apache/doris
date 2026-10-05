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

suite("test_mysql_load_big_file", "p0") {
    sql "show tables"

    def tableName = "test_mysql_load_big_file"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4)  NULL,
            `v2` string  NULL,
            `v3` date  NULL,
            `v4` datetime  NULL,
            INDEX idx_v2 (`v2`) USING INVERTED,
            INDEX idx_v3 (`v3`) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    // If file size is bigger than 16384(16 * 1024), mysql client will spilt it into two packets. In this test, the file size is 17056.
    def mysql_load_skip_lines = getLoalFilePath "test_mysql_load_big_file.csv"

    test {
        // no any skip
        sql """
            LOAD DATA
            LOCAL
            INFILE '${mysql_load_skip_lines}'
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ',';
        """
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                // skip publish timeout
                if (exception.getMessage().contains("PUBLISH_TIMEOUT")) {
                    logger.info(exception.getMessage())
                    return
                }
                throw exception
            }
        }
    }

    sql "sync"
    qt_sql "select k1,k2,count(*) from ${tableName} group by k1, k2 order by k1, k2"
}

