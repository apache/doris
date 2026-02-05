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

suite("count") {
    sql """
        drop table if exists test_count;
    """
    sql """
    create table test_count(
        id int,
        name varchar(20),
        sex int
    ) distributed by hash(id) buckets 1
    properties ("replication_num"="1");
    """
    sql """
    insert into test_count values
        (1, '1', 1),
        (2, '2', 1),
        (3, '3', 1),
        (4, '0', 1),
        (4, '4', 1),
        (5, NULL, 1);
    """
    sql """
    drop table if exists test_insert;
    """
    sql """
    create table test_insert(
        id int,
        name varchar(20),
        sex int
    ) distributed by hash(id) buckets 1
    properties ("replication_num"="1");
    """
    sql """
    insert into test_insert values
        (1, '1', 1),
        (2, '2', 1),
        (3, '3', 1),
        (4, '0', 1),
        (4, '4', 1),
        (5, NULL, 1);
    """
    qt_count_all """select count(*) from test_count;"""
    qt_count_name """select count(name) from test_insert;"""
    qt_count_distinct_sex """select count(distinct sex) from test_insert;"""
    qt_count_distinct_id_sex """select count(distinct id,sex) from test_insert;"""
}
