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

suite("test_mysql_load_tiny_file", "p0") {
    def tableName = "test_mysql_load_tiny_file"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` INT,
            `v5` INT SUM
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`k1`)
        (PARTITION partition_a VALUES [("-10240000"), ("100000")),
        PARTITION partition_b VALUES [("100000"), ("1000000000")),
        PARTITION partition_d VALUES [("1000000000"), (MAXVALUE)))
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    def test_mysql_load_tiny_file = getLoalFilePath "test_mysql_load_tiny_file.csv"

    for (int i = 0; i < 20; i++) {
        test {
            sql """
                LOAD DATA 
                LOCAL
                INFILE '${test_mysql_load_tiny_file}'
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY '\\t';
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
    }

    sql "sync"
    qt_sql "select * from ${tableName} order by k1"
}

