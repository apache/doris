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


suite("test_distributed_string_type") {

    sql "DROP TABLE IF EXISTS all_str"
    sql """
            CREATE TABLE `all_str` (
            `k1` TINYINT NULL,
            `str1` string NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`str1`) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
	"""
    sql "insert into all_str values (1, 'a')"
    sql "insert into all_str values (2, 'b')"
    sql "insert into all_str values (3, 'c')"
    qt_select1 "select * from all_str order by k1"
    qt_select2 "select str1 from all_str group by str1 order by str1"
    qt_select3 "select max(str1) from all_str"

    sql "DROP TABLE IF EXISTS all_str2"
    // string type should could not be key
    test {
        sql """
                CREATE TABLE `all_str2` (
                `k1` TINYINT NULL,
                `str1` string NULL
                ) ENGINE=OLAP
                UNIQUE KEY(`k1`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`str1`) BUCKETS 4
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );
            """
        exception "Distribution column[str1] is not key column"
    }

    sql "DROP TABLE IF EXISTS all_str3"
    test {
        sql """
                CREATE TABLE `all_str3` (
                `k1` TINYINT NULL,
                `str1` string  max NULL
                ) ENGINE=OLAP
                AGGREGATE KEY(`k1`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`str1`) BUCKETS 4
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
                );
            """
        exception "Distribution column[str1] is not key column"
    }
}

