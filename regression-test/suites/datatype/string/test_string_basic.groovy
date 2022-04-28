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

suite("test_string_basic", "datatype") {
    sql "drop table if exists fail_tb1"
    // first column could not be string
    test {
        sql """CREATE TABLE fail_tb1 (k1 STRING NOT NULL, v1 STRING NOT NULL) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")"""
        exception "The olap table first column could not be float, double, string use decimal or varchar instead."
    }
    // string type should could not be key
    test {
        sql """
            CREATE TABLE fail_tb1 ( k1 INT NOT NULL, k2 STRING NOT NULL)
            DUPLICATE KEY(k1,k2) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
            """
        exception "String Type should not be used in key column[k2]"
    }
    // create table with string column, insert and select ok
    def tbName = "str_tb"
    sql "drop table if exists ${tbName}"
    sql """
        CREATE TABLE ${tbName} (k1 VARCHAR(10) NULL, v1 STRING NULL) 
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """
        INSERT INTO ${tbName} VALUES
         ("", ""),
         (NULL, NULL),
         (1, repeat("test1111", 8192)),
         (2, repeat("test1111", 131072))
        """
    order_qt_select_str_tb "select k1, md5(v1), length(v1) from ${tbName}"
}

