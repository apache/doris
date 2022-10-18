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
suite("test_bloom_filter_is_not_null") {
    def table_name = "test_bloom_filter_is_not_null"

    sql """drop TABLE if exists ${table_name}"""

    sql """CREATE TABLE ${table_name} (
      `a` varchar(150) NULL
    ) ENGINE=OLAP
    AGGREGATE KEY(`a`)
    DISTRIBUTED BY HASH(`a`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "bloom_filter_columns" = "a",
    "in_memory" = "false",
    "storage_format" = "V2"
    )"""

    sql """INSERT INTO ${table_name} values (null), ('b')"""

    qt_select_all """select * from ${table_name} order by a"""
    qt_select_not_null """select * from ${table_name} WHERE a is not null"""
    qt_select_null """select * from ${table_name} WHERE a is null"""
}