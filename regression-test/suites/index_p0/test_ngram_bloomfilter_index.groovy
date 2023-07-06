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

    sql "insert into ${tableName} values (1, 'dt_bjn001', 'p9-webcast-sign.douyinpic.com', 'test','/%/7212503657802320699%','/test', 100, false);"

    sql "set enable_function_pushdown = true"

    qt_select_eq "SELECT * FROM ${tableName} WHERE http_url = '/%/7212503657802320699%'"
    qt_select_like "SELECT * FROM ${tableName} WHERE http_url like '/%/7212503657802320699%'"
}
