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

suite("test_mysql_load_unified", "p0") {
    sql "show tables"

    def tableName = "test_mysql_load_unified"

    def mysql_load_skip_lines = getLoalFilePath "mysql_load_trim_quotes.csv"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4)  NULL,
            `v2` string  NULL,
            `v3` date  NULL,
            `v4` datetime  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // trim quotes
    sql """
        LOAD DATA
        LOCAL
        INFILE '${mysql_load_skip_lines}'
        INTO TABLE ${tableName}
        COLUMNS TERMINATED BY ','
        PROPERTIES ("trim_double_quotes"="true");
    """

    sql "sync"
    qt_sql "select * from ${tableName} order by k1, k2"

    // test unified load
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"

        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(20) NULL,
                `k2` bigint(20) NULL,
                `v1` tinyint(4)  NULL,
                `v2` string  NULL,
                `v3` date  NULL,
                `v4` datetime  NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        // trim quotes
        sql """
            LOAD DATA
            LOCAL
            INFILE '${mysql_load_skip_lines}'
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ','
            PROPERTIES ("trim_double_quotes"="true");
        """

        sql "sync"
        qt_sql "select * from ${tableName} order by k1, k2"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

}

