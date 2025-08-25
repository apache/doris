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

suite('test_complextype_to_json', "query_p0") {
    // do support in nereids
    sql """ set experimental_enable_nereids_planner=true"""
    sql """ set enable_fallback_to_original_planner=false; """

    // literal cast
    qt_select """SELECT CAST([] AS JSON)"""
    qt_select """SELECT CAST([123, 456] AS JSON)"""
    qt_select """SELECT CAST(["abc", "def"] AS JSON)"""
    qt_select """SELECT CAST([null, true, false, 100, 6.18, "abc"] AS JSON)"""
    qt_select """SELECT CAST(struct('a', 1, 'doris', 'aaaaa', 1.32) AS JSON)"""
    // invalid map key cast
    test {
        sql """SELECT CAST(map(1, 'a', 2, 'b') AS JSON)"""
        exception "errCode = 2,"
    }
    test {
        sql """SELECT CAST([{1:"v41", 2: 400}] AS JSON)"""
        exception "errCode = 2,"
    }


    sql """ DROP TABLE IF EXISTS test_agg_to_json; """
        sql """
           CREATE TABLE `test_agg_to_json` (
            `id` int(11) NOT NULL,
            `label_name` varchar(32) default null,
            `value_field` string default null
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
        """

    sql """
    insert into `test_agg_to_json` values
    (1, "alex",NULL),
    (1, "LB", "V1_2"),
    (1, "LC", "V1_3"),
    (2, "LA", "V2_1"),
    (2, "LB", "V2_2"),
    (2, "LC", "V2_3"),
    (3, "LA", "V3_1"),
    (3, NULL, NULL),
    (3, "LC", "V3_3"),
    (4, "LA", "V4_1"),
    (4, "LB", "V4_2"),
    (4, "LC", "V4_3"),
    (5, "LA", "V5_1"),
    (5, "LB", "V5_2"),
    (5, "LC", "V5_3"),
    (5, NULL, "V5_3"),
    (6, "LC", "V6_3"),
    (6, "LC", NULL),
    (6, "LC", "V6_3"),
    (6, "LC", NULL),
    (6, NULL, "V6_3"),
    (7, "LC", "V7_3"),
    (7, "LC", NULL),
    (7, "LC", "V7_3"),
    (7, "LC", NULL),
    (7, NULL, "V7_3");
    """

    // array_agg result cast to json then combination to json_object
    qt_sql_arr_agg_cast """ select t.id, cast(t.label_name as json), cast(t.value_field as json) from (select id, array_agg(label_name) as label_name, array_agg(value_field) as value_field from test_agg_to_json group by id) t order by t.id; """
    qt_sql_arr_agg_cast_json_object """ select json_object("id", t.id, "label", cast(t.label_name as json), "field", cast(t.value_field as json)) from (select id, array_agg(label_name) as label_name, array_agg(value_field) as value_field from test_agg_to_json group by id) t order by t.id; """



    sql """
        set debug_skip_fold_constant = true;
    """

    sql """
        set enable_strict_cast = true;
    """

    // cast from string to json


    qt_sql_cast_string_to_json """ select cast('{"k1":1, "k2":"2"}' as json) """
    qt_sql_cast_string_to_json """ select cast('232323' as json) """



    test {
        sql """  SELECT CAST('{invalid JSON' AS JSON); """
        exception "INVALID_ARGUMENT"
    }


    sql """
        drop table if exists test_json_from_string;
    """

    sql """
      CREATE TABLE IF NOT EXISTS test_json_from_string (
          id INT not null,
          j string  null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
    """

    sql """

        INSERT INTO test_json_from_string VALUES
        (1, '{"k1":1, "k2":"2"}'),
        (2, '232323'),
        (3, null),
        (4, null),
        (5, '{"k1":1, "k2":"2", "k3": {"k4": 4}, "k5": [1, 2, 3]}');
    """
    
    qt_sql_json_from_string """ SELECT id, CAST(j AS JSON) FROM test_json_from_string ORDER BY id; """
}
