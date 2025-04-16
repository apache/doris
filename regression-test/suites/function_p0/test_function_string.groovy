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

suite("test_function_string") {

    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false; """

    sql """
        drop table if exists test_tb_function_space;
    """

    sql """
    CREATE TABLE `test_tb_function_space` (
        `k1` bigint NULL,
        `k2` text NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
        insert into test_tb_function_space values ('-126', 'a'),('-125', 'a'),('-124', 'a'),('-123', 'a'),('-121', 'a'),('2', 'a');
    """

    qt_sql """ 
        select concat(space(k1), k2) as k from test_tb_function_space order by k;
    """

    sql """
        drop table if exists test_tb_function_space;
    """


    sql """
        drop table if exists test_parse_url;
    """

    sql """
     CREATE TABLE `test_parse_url` (
        `id` int NULL,
        `url` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into test_parse_url values (1, 'http://www.facebook.com'), (2, "http://www.google.com/test?name=abc&age=20");
    """

    qt_sql """
        select parse_url(url, 'HOST') as host, parse_url(url, 'FILE') as file from test_parse_url order by id;
    """


    sql """
        set DEBUG_SKIP_FOLD_CONSTANT = true;
    """
    qt_sql """
       select initcap('GROSSE     àstanbul , ÀÇAC123    ΣΟΦΟΣ');
    """
    sql """
        set DEBUG_SKIP_FOLD_CONSTANT = false;
    """
}
