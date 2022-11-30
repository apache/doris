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

suite("alter_column_stats") {
    sql """DROP TABLE IF EXISTS statistics_test"""
    sql """
        CREATE TABLE statistics_test (
            `id` BIGINT,
            `col1` VARCHAR,
            `col2` DATE
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES (
            "replication_num"="1"
        );
    """

    sql """INSERT INTO statistics_test VALUES(1, 'a', '2011-01-01')"""
    sql """INSERT INTO statistics_test VALUES(2, 'b', '2012-01-01')"""
    sql """INSERT INTO statistics_test VALUES(3, 'c', '2013-01-01')"""

    sql """ANALYZE statistics_test"""

    sleep(9000)

    qt_sql """
        SHOW COLUMN STATS statistics_test
    """

    sql """
            ALTER TABLE statistics_test
            MODIFY COLUMN col1 SET STATS('ndv'='148064528', 'num_nulls'='0', 'min_value'='1', 'max_value'='6',
            'row_count'='114', 'data_size'='511');
        """

    qt_sql2 """
        SHOW COLUMN STATS statistics_test
    """
}
