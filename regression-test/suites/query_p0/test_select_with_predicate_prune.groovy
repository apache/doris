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
suite("test_select_with_predicate_prune") {
    sql """
        drop table if exists `test_select_with_predicate_prune`;
    """
    sql """
        CREATE TABLE IF NOT EXISTS `test_select_with_predicate_prune` (
            id int,
            name string,
            birthday date not null
        )
        duplicate key(`id`)
        PARTITION BY RANGE (`birthday`)(
            PARTITION `p20201001` VALUES LESS THAN ("2020-10-02"),
            PARTITION `p20201002` VALUES LESS THAN ("2020-10-03"),
            PARTITION `p20201003` VALUES LESS THAN ("2020-10-04")
        )
        DISTRIBUTED BY HASH(`id`) buckets 1
        PROPERTIES
        (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into test_select_with_predicate_prune values (1, 'jerry', '2020-10-01'), (2, 'tom', '2020-10-02');
    """
    sql """
        insert into test_select_with_predicate_prune values (3, 'jack', '2020-10-01'), (4, 'tony', '2020-10-02');
    """

    qt_select1 """
        select * from test_select_with_predicate_prune where birthday < '2020-10-03' order by id;
    """

    qt_select2 """
        select * from test_select_with_predicate_prune where birthday < '2020-10-02' order by id;
    """

    qt_select3 """
        select * from test_select_with_predicate_prune where birthday < '2020-10-01' order by id;
    """


    qt_select4 """
        select * from test_select_with_predicate_prune where birthday > '2020-09-30' order by id;
    """

    qt_select5 """
        select * from test_select_with_predicate_prune where birthday > '2020-10-01' order by id;
    """

    qt_select6 """
        select * from test_select_with_predicate_prune where birthday > '2020-10-02' order by id;
    """
}