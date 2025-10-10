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

suite("test_bitmap_and") {
    sql """ DROP TABLE IF EXISTS unique_user_tags_mor; """
    sql """
            CREATE TABLE unique_user_tags_mor (
                user_id BIGINT,
                visit_date DATE,
                tags_bitmap BITMAP COMMENT '用户标签Bitmap'
            )
            UNIQUE KEY(user_id, visit_date)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false"
            );
    """

    sql """
        INSERT INTO unique_user_tags_mor VALUES 
        (1001, '2023-10-01', to_bitmap(101)),
        (1002, '2023-10-01', to_bitmap(102)),
        (1003, '2023-10-01', to_bitmap(103));
    """

    sql """
        INSERT INTO unique_user_tags_mor VALUES 
        (1001, '2023-10-01', bitmap_or(to_bitmap(101), to_bitmap(102)));
    """
    sql """
        INSERT INTO unique_user_tags_mor VALUES
        (1001, '2023-10-01', bitmap_from_string('101,102,103')),
        (1002, '2023-10-01', bitmap_from_string('102,104')),
        (1003, '2023-10-01', bitmap_from_string('101,103,105')),
        (1004, '2023-10-01', bitmap_from_string('104,106')),
        (1005, '2023-10-01', bitmap_from_string('101,102,103,104')),
        (1006, '2023-10-01', bitmap_from_string('105,106')),
        (1007, '2023-10-01', bitmap_from_string('101,107')),
        (1008, '2023-10-01', bitmap_from_string('102,103,108')),
        (1009, '2023-10-01', bitmap_from_string('104,105,109')),
        (1010, '2023-10-01', bitmap_from_string('101,110'));
    """
    sql """
        INSERT INTO unique_user_tags_mor VALUES
        (1001, '2023-10-02', bitmap_from_string('101,102,104')),
        (1002, '2023-10-02', bitmap_from_string('102,103,105')),
        (1003, '2023-10-02', bitmap_from_string('101,106')),
        (1004, '2023-10-02', bitmap_from_string('104,107')),
        (1005, '2023-10-02', bitmap_from_string('102,103,108')),
        (1006, '2023-10-02', bitmap_from_string('105,106,109')),
        (1007, '2023-10-02', bitmap_from_string('101,110')),
        (1008, '2023-10-02', bitmap_from_string('103,104')),
        (1009, '2023-10-02', bitmap_from_string('105,107')),
        (1010, '2023-10-02', bitmap_from_string('101,102,108'));
    """
    sql """
        INSERT INTO unique_user_tags_mor VALUES
        (1001, '2023-10-03', bitmap_from_string('101,103,105')),
        (1002, '2023-10-03', bitmap_from_string('102,104,106')),
        (1003, '2023-10-03', bitmap_from_string('107,108')),
        (1004, '2023-10-03', bitmap_from_string('104,105,109')),
        (1005, '2023-10-03', bitmap_from_string('101,110')),
        (1006, '2023-10-03', bitmap_from_string('102,103')),
        (1007, '2023-10-03', bitmap_from_string('104,105')),
        (1008, '2023-10-03', bitmap_from_string('106,107')),
        (1009, '2023-10-03', bitmap_from_string('108,109')),
        (1010, '2023-10-03', bitmap_from_string('101,110'));

    """

    qt_sql_bitmap_and_1 """ SELECT bitmap_count(
            bitmap_andnot(
                bitmap_or(
                    tags_bitmap,
                    bitmap_from_string('100,200,300')
                ),
                bitmap_and(
                    tags_bitmap,
                    bitmap_from_string('101,102,103')
                )
            )
        )
        FROM unique_user_tags_mor
        WHERE user_id = 1001 order by user_id;
 """
    qt_sql_bitmap_and_2 """ select              bitmap_and(                 tags_bitmap,                 bitmap_from_string('101,102,103')             ) FROM     unique_user_tags_mor WHERE     user_id = 1001 order by user_id; """
}
