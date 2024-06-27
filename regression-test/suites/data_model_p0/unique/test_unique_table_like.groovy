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

suite("test_unique_table_like") {
    def dbName = "test_unique_like_db"
    sql "drop database if exists ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "use ${dbName}"

    // test uniq table like 
    def tbNameA = "test_uniq"
    def tbNameB = "test_uniq_like"
    sql "SET show_hidden_columns=true"
    sql "DROP TABLE IF EXISTS ${tbNameA}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbNameA} (
                k int,
                int_value int,
                char_value char(10),
                date_value date
            )
            ENGINE=OLAP
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1",
                "function_column.sequence_type" = "int");
        """
    qt_desc_uniq_table "desc ${tbNameA}"    
    sql """
            CREATE TABLE IF NOT EXISTS ${tbNameB} LIKE ${tbNameA};
        """
    
    qt_desc_uniq_table "desc ${tbNameB}"
    sql "DROP TABLE ${tbNameA}"
    sql "DROP TABLE ${tbNameB}"
}

