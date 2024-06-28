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
suite("test_index_chinese_column", "inverted_index_select"){
    def createAndInsertData = { table_name, inverted_index_storage_format ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE ${table_name}
            (
                k1 int ,
                名称 string,
                k3 char(50),
                k4 varchar(200),
                k5 datetime,
                index index_str_k2 (`名称`) using inverted properties("parser"="english","ignore_above"="257")
            )
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES("replication_num" = "1","inverted_index_storage_format" = "${inverted_index_storage_format}")
        """
        sql " insert into ${table_name} values(1, 'json love anny', 'json', 'anny', '2023-10-10 12:11:11') "
        qt_sql "SELECT * FROM ${table_name} WHERE 名称 match_all 'json'"
    }

    def table_name_v1 = "test_index_chinese_column_v1"
    def table_name_v2 = "test_index_chinese_column_v2"

    sql "set enable_unicode_name_support=true"

    createAndInsertData(table_name_v1, "V1")
    createAndInsertData(table_name_v2, "V2")
}
