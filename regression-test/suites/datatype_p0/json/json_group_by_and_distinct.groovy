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

suite("test_json_group_by_and_distinct", "p0") {
    

    sql """
        drop table if exists test_jsonb_groupby;
    """
    sql """
    CREATE TABLE IF NOT EXISTS test_jsonb_groupby (
              `id` INT ,
              `j` jsonb
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """


    sql """
        insert into test_jsonb_groupby values (1, '{"a":1, "b":2}'), (2, '{"a":1, "b":3}'), (3, '{"a":2, "b":2}') , (4, '{"a":2, "b":2}') , (5, '{"a":1, "b":2}') , (6, '{"a":2, "b":2}') ;
    """

    qt_order"""
        select j, count(*) as cnt from test_jsonb_groupby group by j order by cnt desc, cast(j as string);
    """

    qt_order"""
        select distinct j from test_jsonb_groupby order by cast(j as string);
    """


    sql """
        drop table if exists test_jsonb_obj;
    """

    sql """
         CREATE TABLE IF NOT EXISTS test_jsonb_obj (
              `id` INT ,
              `j` jsonb
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """

    sql """
        insert into test_jsonb_obj values (1,'{"a":1, "b":2}'), (2,'{"b":2, "a":1}');
    """

    qt_order"""
        select j from test_jsonb_obj group by j order by cast(j as string);
    """

    qt_order"""
        select SORT_JSON_OBJECT_KEYS(j), count(*) from test_jsonb_obj group by SORT_JSON_OBJECT_KEYS(j) order by cast(SORT_JSON_OBJECT_KEYS(j) as string);
    """




    sql """
        drop table if exists test_jsonb_number;
    """

    sql """
         CREATE TABLE IF NOT EXISTS test_jsonb_number (
              `id` INT ,
              `j` jsonb
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """


    sql """
        insert into test_jsonb_number values (1,to_json( cast(123 as bigint))), (2,to_json(cast(123 as tinyint)));
    """

    qt_order"""
        select j from test_jsonb_number group by j order by cast(j as string);
    """

    qt_order"""
        select NORMALIZE_JSON_NUMBERS_TO_DOUBLE(j), count(*) from test_jsonb_number group by NORMALIZE_JSON_NUMBERS_TO_DOUBLE(j) order by cast(NORMALIZE_JSON_NUMBERS_TO_DOUBLE(j) as string);
    """
}
