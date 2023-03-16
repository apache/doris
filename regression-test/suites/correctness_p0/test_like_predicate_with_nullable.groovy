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

suite("test_like_predicate_with_nullable") {
    def tableName = "test_like_predicate_with_nullable"


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            `id` INT,
            `url` STRING NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO ${tableName} VALUES 
            (21,"http://irr.ru/index.php?showalbum/login-kupalnik.ru/exp?sid=3205"),
            (22, null),
            (22,"http://irr.ru/index.php?showalbum/login-kupalnik.10065%26bn%3D0%26ad%3D158197%26ar_sliceid%3DBqNsiYI9Hg"),
            (23,"http://acat.php?cid=577&oki=1&op_product_id=9555768&wi=11579][to]=&int[112898/Realty/art/416594,90682/google.ru/search"),
            (24,"http://smeshariki.ru/googleTBR%26ar_ntype=citykurortmag"),
            (26, null),
            (25, null); """

    qt_select1 """ select id, url from ${tableName} order by id, url; """
    qt_select2 """ select id, url from ${tableName} where url is null; """
    qt_select3 """ select id, url from ${tableName} where url like '%google%' order by id, url; """
    qt_select4 """ select id, url from ${tableName} where url like '%google%' and url is null; """
    qt_select5 """ select id, url from ${tableName} where url like '%google%' and url is not null order by id, url; """
}
