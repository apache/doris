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

suite("test_array_index_parser", "nonConcurrent"){

    def create_array_index_table = {testTablex, parser ->
        sql "DROP TABLE IF EXISTS " + testTablex + ";"
        def stmt = "CREATE TABLE IF NOT EXISTS " + testTablex + "(\n" +
                "k1 INT NULL,\n c_arr ARRAY<STRING> NULL,\n"

        String strTmp = parser == "" ? "INDEX index_inverted_c_arr(c_arr) USING INVERTED COMMENT 'c_arr index',\n" :
                "INDEX index_inverted_c_arr(c_arr) USING INVERTED PROPERTIES( \"parser\"=\"" + parser + "\") COMMENT 'c_arr index',\n"

        stmt += strTmp
        stmt = stmt.substring(0, stmt.length()-2)
        stmt += ") \nENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT 'OLAP'\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\");"
        return stmt
    }
    test {
        def stmt = create_array_index_table.call("test_array_index_parser_english", "english")
        sql stmt
        exception ("INVERTED index with parser")
    }
    test {
        def stmt = create_array_index_table.call("test_array_index_parser_chinese", "chinese")
        sql stmt
        exception ("INVERTED index with parser")
    }
    test {
        def stmt = create_array_index_table.call("test_array_index_parser_unicode", "unicode")
        sql stmt
        exception ("INVERTED index with parser")
    }

    sql create_array_index_table.call("test_array_index_parser_default", "none")
    sql create_array_index_table.call("test_array_index_parser_empty", "")
}
