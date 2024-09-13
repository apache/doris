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

/*
    // Test Case DDL
    create table array_table (
        id int,
        arr1 ARRAY<BIGINT>,
        arr2 ARRAY<VARCHAR(10)>,
        arr3 ARRAY<DOUBLE>,
        arr4 ARRAY<DATE>,
        arr5 ARRAY<DATETIME>
    );
    INSERT INTO array_table VALUES(1, array(1, 2, 3), array('a', 'b', 'c'), array(1.2, 1.3), array(date('2023-05-23')), array(datetime('2023-05-23 13:55:12')));
    INSERT INTO array_table VALUES(1, array(1, 2, 3), array('a', 'b', 'c'), array(1.2, 1.3), array(date('2023-05-23')), array(datetime('2023-05-23 13:55:12')));
    INSERT INTO array_table VALUES(2, array(1, 2, 3), array('a', 'b', 'c'), array(1.2, 1.3), array(date('2023-05-23')), array(datetime('2023-05-23 13:55:12')));
    INSERT INTO array_table VALUES(3, array(1, 2, 3), array('a', 'b', 'c'), array(1.3), array(date('2023-05-23')), array(datetime('2023-05-23 13:55:12')));

    drop table map_table;
    create table map_table (
        id int.
        arr1 MAP<BIGINT, DOUBLE>,
        arr2 MAP<BIGINT, STRING>
    );
    INSERT INTO map_table (arr1, arr2)
    VALUES (
        1,
        MAP(1, 2.5, 2, 3.75),
        MAP(1, 'example1', 2, 'example2')
    );
    INSERT INTO map_table (arr1, arr2)
    VALUES (
        2,
        MAP(3, 2.5, 99, 3.75),
        MAP(349, 'asd', 324, 'uid')
    );
    drop table struct_table;
    create table struct_table (
        id int,
        user_info STRUCT<id: STRING,age: INT>,
        contact_info STRUCT<phone_number: BIGINT, email: STRING, addr: VARCHAR(10)>
    );

    INSERT INTO struct_table VALUES
    (
        1,
        named_struct('id', 'user1', 'age', 25),
        named_struct('phone_number', 123450, 'email', 'user1@example.com', 'addr', 'Addr1')
    ),
    (
        2,
        named_struct('id', 'user2', 'age', 30),
        named_struct('phone_number', 2345671, 'email', 'user2@example.com', 'addr', 'Addr2')
    ),
    (
        3,
        named_struct('id', 'user3', 'age', 35),
        named_struct('phone_number', 3456789, 'email', 'user3@example.com', 'addr', 'Addr3')
    );

    CREATE TABLE nested_complex_table (
        user_id STRING,
        user_profile STRUCT<
            name: STRING,
            age: INT,
            preferences: MAP<
                STRING,
                STRUCT<
                    preference_id: INT,
                    preference_values: ARRAY<STRING>
                >
            >
        >,
        activity_log ARRAY<
            STRUCT<
                activity_date: STRING,
                activities: MAP<
                    STRING,
                    STRUCT<
                        details: STRING,
                        metrics: MAP<STRING, DOUBLE>
                    >
                >
            >
        >
    );
    INSERT INTO nested_complex_table VALUES
    (
        'user1',
        named_struct('name', 'Alice', 'age', 28, 'preferences', map(
            'sports', named_struct('preference_id', 101, 'preference_values', array('soccer', 'tennis')),
            'music', named_struct('preference_id', 102, 'preference_values', array('rock', 'classical'))
        )),
        array(
            named_struct('activity_date', '2024-08-01', 'activities', map(
                'workout', named_struct('details', 'Morning run', 'metrics', map('duration', 30.5, 'calories', 200.0)),
                'reading', named_struct('details', 'Read book on Hive', 'metrics', map('pages', 50.0, 'time', 2.0))
            )),
            named_struct('activity_date', '2024-08-02', 'activities', map(
                'travel', named_struct('details', 'Flight to NY', 'metrics', map('distance', 500.0, 'time', 3.0)),
                'meeting', named_struct('details', 'Project meeting', 'metrics', map('duration', 1.5, 'participants', 5.0))
            ))
        )
    ),
    (
        'user2',
        named_struct('name', 'Bob', 'age', 32, 'preferences', map(
            'books', named_struct('preference_id', 201, 'preference_values', array('fiction', 'non-fiction')),
            'travel', named_struct('preference_id', 202, 'preference_values', array('beaches', 'mountains'))
        )),
        array(
            named_struct('activity_date', '2024-08-01', 'activities', map(
                'hiking', named_struct('details', 'Mountain trail', 'metrics', map('distance', 10.0, 'elevation', 500.0)),
                'photography', named_struct('details', 'Wildlife photoshoot', 'metrics', map('photos_taken', 100.0, 'time', 4.0))
            )),
            named_struct('activity_date', '2024-08-02', 'activities', map(
                'workshop', named_struct('details', 'Photography workshop', 'metrics', map('duration', 3.0, 'participants', 15.0)),
                'shopping', named_struct('details', 'Bought camera gear', 'metrics', map('items', 5.0, 'cost', 1500.0))
            ))
        )
    ),
    (
        'user3',
        named_struct('name', 'Carol', 'age', 24, 'preferences', map(
            'food', named_struct('preference_id', 301, 'preference_values', array('vegan', 'desserts')),
            'movies', named_struct('preference_id', 302, 'preference_values', array('action', 'comedy'))
        )),
        array(
            named_struct('activity_date', '2024-08-01', 'activities', map(
                'cooking', named_struct('details', 'Made vegan meal', 'metrics', map('time_spent', 1.5, 'calories', 500.0)),
                'movie', named_struct('details', 'Watched action movie', 'metrics', map('duration', 2.0, 'rating', 8.5))
            )),
            named_struct('activity_date', '2024-08-02', 'activities', map(
                'gym', named_struct('details', 'Strength training', 'metrics', map('duration', 1.0, 'calories', 300.0)),
                'shopping', named_struct('details', 'Bought groceries', 'metrics', map('items', 10.0, 'cost', 100.0))
            ))
        )
    );
 */
suite("test_max_compute_complex_type", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("aliYunAk")
        String sk = context.config.otherConfigs.get("aliYunSk")
        String mc_catalog_name = "test_max_compute_complex_type"
        sql """drop catalog if exists ${mc_catalog_name} """
        sql """
        CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
                "type" = "max_compute",
                "mc.default.project" = "jz_datalake",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "http://service.cn-beijing-vpc.maxcompute.aliyun-inc.com/api"
        );
        """

        logger.info("catalog " + mc_catalog_name + " created")
        sql """switch ${mc_catalog_name};"""
        logger.info("switched to catalog " + mc_catalog_name)
        sql """ use jz_datalake """

        qt_mc_q1 """ select id,arr3,arr1,arr5,arr2 from array_table order by id desc """
        qt_mc_q2 """ select arr2,arr1 from map_table order by id limit 2 """
        qt_mc_q3 """ select contact_info,user_info from struct_table order by id limit 2 """
        qt_mc_q4 """ select user_id,activity_log from nested_complex_table order by user_id limit 2 """

        sql """drop catalog ${mc_catalog_name};"""
    }
}
