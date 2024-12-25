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
suite("test_ngram_bloomfilter_index") {
    // todo: test bitmap index, such as create, drop, alter table index
    def tableName = 'test_ngram_bloomfilter_index'
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
        `key_id` bigint(20) NULL COMMENT '',
        `category` varchar(200) NULL COMMENT '',
        `https_url` varchar(300) NULL COMMENT '',
        `hostname` varchar(300) NULL,
        `http_url` text NULL COMMENT '',
        `url_path` varchar(2000) NULL COMMENT '',
        `cnt` bigint(20) NULL COMMENT '',
        `host_flag` boolean NULL COMMENT '',
        INDEX idx_ngrambf (`http_url`) USING NGRAM_BF PROPERTIES("gram_size" = "2", "bf_size" = "512")
    ) ENGINE=OLAP
    DUPLICATE KEY(`key_id`, `category`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`key_id`) BUCKETS 3
    PROPERTIES("replication_num" = "1");
    """

    sql "INSERT INTO ${tableName} values (1, 'dt_bjn001', 'p9-webcast-sign.douyinpic.com', 'test', '/%/7212503657802320699%', '/test', 100, false);"
    sql "INSERT INTO ${tableName} values (2, 'dt_bjn001', 'p9-webcast-sign.douyinpic.com', 'test', '/%/7212503657802320699%xxx', '/test', 100, false);"


    sql "SET enable_function_pushdown = true"

    qt_select_all_1 "SELECT * FROM ${tableName} ORDER BY key_id"
    qt_select_eq_1 "SELECT * FROM ${tableName} WHERE http_url = '/%/7212503657802320699%' ORDER BY key_id"
    qt_select_in_1 "SELECT * FROM ${tableName} WHERE http_url IN ('/%/7212503657802320699%') ORDER BY key_id"
    qt_select_like_1 "SELECT * FROM ${tableName} WHERE http_url like '/%/7212503657802320699%' ORDER BY key_id"

    // delete and then select
    sql "DELETE FROM ${tableName} WHERE http_url IN ('/%/7212503657802320699%')"
    qt_select_all_2 "SELECT * FROM ${tableName} ORDER BY key_id"
    qt_select_eq_2 "SELECT * FROM ${tableName} WHERE http_url = '/%/7212503657802320699%' ORDER BY key_id"
    qt_select_in_2 "SELECT * FROM ${tableName} WHERE http_url IN ('/%/7212503657802320699%') ORDER BY key_id"
    qt_select_like_2 "SELECT * FROM ${tableName} WHERE http_url like '/%/7212503657802320699%' ORDER BY key_id"

    sql "DELETE FROM ${tableName} WHERE http_url = '/%/7212503657802320699%xxx'"
    qt_select_all_3 "SELECT * FROM ${tableName} ORDER BY key_id"
    qt_select_eq_3 "SELECT * FROM ${tableName} WHERE http_url = '/%/7212503657802320699%' ORDER BY key_id"
    qt_select_in_3 "SELECT * FROM ${tableName} WHERE http_url IN ('/%/7212503657802320699%') ORDER BY key_id"
    qt_select_like_3 "SELECT * FROM ${tableName} WHERE http_url like '/%/7212503657802320699%' ORDER BY key_id"

    //case for bf_size 65536
    def tableName2 = 'test_ngram_bloomfilter_index2'
    sql "DROP TABLE IF EXISTS ${tableName2}"
    test {
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
            `key_id` bigint(20) NULL COMMENT '',
            `category` varchar(200) NULL COMMENT '',
            `https_url` varchar(300) NULL COMMENT '',
            `hostname` varchar(300) NULL,
            `http_url` text NULL COMMENT '',
            `url_path` varchar(2000) NULL COMMENT '',
            `cnt` bigint(20) NULL COMMENT '',
            `host_flag` boolean NULL COMMENT '',
            INDEX idx_ngrambf (`http_url`) USING NGRAM_BF PROPERTIES("gram_size" = "2", "bf_size" = "65536")
        ) ENGINE=OLAP
        DUPLICATE KEY(`key_id`, `category`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`key_id`) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
        exception "bf_size should be integer and between 64 and 65535"
    }

    def tableName3 = 'test_ngram_bloomfilter_index3'
    sql "DROP TABLE IF EXISTS ${tableName3}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName3} (
            `key_id` bigint(20) NULL COMMENT '',
            `category` varchar(200) NULL COMMENT '',
            `https_url` varchar(300) NULL COMMENT '',
            `hostname` varchar(300) NULL,
            `http_url` text NULL COMMENT '',
            `url_path` varchar(2000) NULL COMMENT '',
            `cnt` bigint(20) NULL COMMENT '',
            `host_flag` boolean NULL COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`key_id`, `category`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`key_id`) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
    test {
        sql """ALTER TABLE  ${tableName3} ADD INDEX idx_http_url(http_url) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="65536") COMMENT 'http_url ngram_bf index'"""
        exception "'bf_size' should be an integer between 64 and 65535"
    }
    test {
        sql """ALTER TABLE  ${tableName3} ADD INDEX idx_http_url(http_url) USING NGRAM_BF PROPERTIES("gram_size"="256", "bf_size"="65535") COMMENT 'http_url ngram_bf index'"""
        exception "'gram_size' should be an integer between 1 and 255"
    }
}
