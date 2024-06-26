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

suite("test_array_functions") {
    def tableName = "tbl_test_array_functions"
    // array functions only supported in vectorized engine
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL COMMENT "",
              `k2` ARRAY<int(11)> NOT NULL COMMENT "",
              `k3` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k4` ARRAY<int(11)> NULL COMMENT "",
              `k5` ARRAY<CHAR(5)> NULL COMMENT "",
              `k6` ARRAY<date> NULL COMMENT "",
              `k7` ARRAY<datetime> NULL COMMENT "",
              `k8` ARRAY<datev2> NULL COMMENT "",
              `k9` ARRAY<datev2> NULL COMMENT "",
              `k10` ARRAY<datetimev2(3)> NULL COMMENT "",
              `k11` ARRAY<datetimev2(3)> NULL COMMENT "",
              `k12` ARRAY<decimalv3(6, 3)> NULL COMMENT "",
              `k13` ARRAY<decimalv3(6, 3)> NULL COMMENT "",
              `k14` ARRAY<int(11)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName} VALUES(1,[1,2,3],["a","b",""],[1,2],["hi"],["2015-03-13"],["2015-03-13 12:36:38"],["2023-02-05","2023-02-06"],["2023-02-07","2023-02-06"],['2022-10-15 10:30:00.999', '2022-08-31 12:00:00.999'],['2022-10-16 10:30:00.999', '2022-08-31 12:00:00.999'],[111.111, 222.222],[222.222, 333.333],[1,222,3]) """
    sql """ INSERT INTO ${tableName} VALUES(2,[4],NULL,[5],["hi2"],NULL,NULL,["2023-01-05","2023-01-06"],["2023-01-07","2023-01-06"],['2022-11-15 10:30:00.999', '2022-01-31 12:00:00.999'],['2022-11-16 10:30:00.999', '2022-01-31 12:00:00.999'],[333.3333, 444.4444],[444.4444, 555.5555],NULL) """
    sql """ INSERT INTO ${tableName} VALUES(3,[],[],NULL,["hi3"],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,[2,3,4]) """
    sql """ INSERT INTO ${tableName} VALUES(4,[1,2,3,4,5,4,3,2,1],[],[],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,[NULL, 23]) """
    sql """ INSERT INTO ${tableName} VALUES(5,[],["a","b","c","d","c","b","a"],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,[]) """
    sql """ INSERT INTO ${tableName} VALUES(6,[1,2,3,4,5,4,3,2,1],["a","b","c","d","c","b","a"],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,[NULL, 22]) """
    sql """ INSERT INTO ${tableName} VALUES(7,[8,9,NULL,10,NULL],["f",NULL,"g",NULL,"h"],NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,[8,9,NULL,10,NULL]) """
    sql """ INSERT INTO ${tableName} VALUES(8,[1,2,3,3,4,4,NULL],["a","b","b","b"],[1,2,2,3],["hi","hi","hello"],["2015-03-13"],["2015-03-13 12:36:38"],NULL,NULL,NULL,NULL,NULL,NULL,[8,9,NULL,10,NULL]) """
    sql """ INSERT INTO ${tableName} VALUES(9,[1,2,3],["a","b",""],[1,2],["hi"],["2015-03-13","2015-03-13","2015-03-14"],["2015-03-13 12:36:38","2015-03-13 12:36:38"],NULL,NULL,NULL,NULL,NULL,NULL,[NULL,12]) """

    qt_select "SELECT k1, size(k2), size(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, cardinality(k2), cardinality(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, arrays_overlap(k12, k13) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_distinct(k2), array_distinct(k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_distinct(k8), array_distinct(k10) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_distinct(k12) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k2, k1), k1 FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k8, cast('2023-02-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k10, cast('2022-10-15 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT array_remove(k12, cast(111.111 as decimalv3(6,3))) FROM ${tableName} ORDER BY k1"
    qt_select_array_sort1 "SELECT k1, array_sort(k2), array_sort(k3), array_sort(k4) FROM ${tableName} ORDER BY k1"
    qt_select_array_sort2 "SELECT k1, array_sort(k8), array_sort(k10) FROM ${tableName} ORDER BY k1"
    qt_select_array_sort3 "SELECT k1, array_sort(k12) FROM ${tableName} ORDER BY k1"
    qt_select_array_reverse_sort1 "SELECT k1, array_reverse_sort(k2), array_reverse_sort(k3), array_reverse_sort(k4) FROM ${tableName} ORDER BY k1"
    qt_select_array_reverse_sort2 "SELECT k1, array_reverse_sort(k8), array_reverse_sort(k10) FROM ${tableName} ORDER BY k1"
    qt_select_array_reverse_sort3 "SELECT k1, array_reverse_sort(k12) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_union(k12, k13) FROM ${tableName} ORDER BY k1"
    // multi-params array_union
    qt_select "SELECT k1, array_union(k2, k4, k14) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_except(k12, k13) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k10, k11) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_intersect(k12, k13) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k2, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k2, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k8, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k10, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_slice(k12, 1, 2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, reverse(k2), reverse(k3), reverse(k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, reverse(k8), reverse(k10) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, reverse(k12) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_join(k2, '_', 'null'), array_join(k3, '-', 'null') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k2, 1) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k3, 'a') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k3, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k4, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k5, 'hi') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k5, 'hi222') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k6, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k8, cast('2023-02-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k10, cast('2022-10-15 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_contains(k12, cast(111.111 as decimalv3(6,3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k8) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k10) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate(k12) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate_uniq(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate_uniq(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate_uniq(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate_uniq(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate_uniq(k8) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate_uniq(k10) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_enumerate_uniq(k12) from ${tableName} ORDER BY k1"

    // Here disable the cases about `array_shuffle` since the result of `array_shuffle` is not stable.
    // FYI: the result of `array_shuffle` depends on the row position in the column.
    /*
    qt_select_array_shuffle1 "SELECT k1, k2, array_sum(k2), array_sum(array_shuffle(k2)), array_shuffle(k2, 0), shuffle(k2, 0) from ${tableName} ORDER BY k1"
    qt_select_array_shuffle2 "SELECT k1, k5, array_size(k5), array_size(array_shuffle(k5)), array_shuffle(k5, 0), shuffle(k5, 0) from ${tableName} ORDER BY k1"
    qt_select_array_shuffle3 "SELECT k1, k6, array_size(k6), array_size(array_shuffle(k6)), array_shuffle(k6, 0), shuffle(k6, 0) from ${tableName} ORDER BY k1"
    qt_select_array_shuffle4 "SELECT k1, k7, array_size(k7), array_size(array_shuffle(k7)), array_shuffle(k7, 0), shuffle(k7, 0) from ${tableName} ORDER BY k1"
    qt_select_array_shuffle5 "SELECT k1, k10, array_size(k10), array_size(array_shuffle(k10)), array_shuffle(k10, 0), shuffle(k10, 0) from ${tableName} ORDER BY k1"
    qt_select_array_shuffle6 "SELECT k1, k12, array_sum(k12), array_sum(array_shuffle(k12)), array_shuffle(k12, 1), shuffle(k12, 1) from ${tableName} ORDER BY k1"
    */
    qt_select "SELECT k1, array_popback(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k8) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k10) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popback(k12) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k7) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k8) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k10) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_popfront(k12) from ${tableName} ORDER BY k1"
    qt_select_array_with_constant1 "SELECT k1, array_with_constant(3, k1),  array_repeat(k1, 3) from ${tableName} ORDER BY k1"
    qt_select_array_with_constant2 "SELECT k1, array_with_constant(10, null), array_repeat(null, 10) from ${tableName} ORDER BY k1"
    qt_select_array_with_constant3 "SELECT k1, array_with_constant(2, 'a'), array_repeat('a', 2) from ${tableName} ORDER BY k1"
    qt_select_array_with_constant4 "SELECT k1, array_with_constant(2, 123), array_repeat(123, 2) from ${tableName} ORDER BY k1"
    qt_select_array_with_constant5 "SELECT k1, array_with_constant(k1, 3), array_repeat(3, k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array(2, k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array(k1, null, '2020-01-01') from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array(null, k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k2, 5) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k3, 'a') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k3, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k4, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k5, 'hi') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k5, 'hi222') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k6, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k8, cast('2023-02-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k10, cast('2022-10-15 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_position(k12, cast(111.111 as decimalv3(6,3))) FROM ${tableName} ORDER BY k1"
    // array_position without cast function
    qt_select "SELECT k1, array_position(k12, 111.111) FROM ${tableName} ORDER BY k1"

    qt_select_array "SELECT k1, array(k1), array_contains(array(k1), k1) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k2, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k2, [1, null, 2], k4, [null]) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k8, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k10, k11, array(cast('2023-03-05 10:30:00.999' as datetimev2(3)))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_concat(k12, k13) FROM ${tableName} ORDER BY k1"

    qt_select "SELECT k1, array_zip(k2, k2, k2) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k3, k3) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k4, k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k5, k5) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k6, k6) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k7, k7) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k8, k8) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k9, k9) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k10, k10) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_zip(k11, k11, k11, k11) FROM ${tableName} ORDER BY k1"

    qt_select "SELECT k1, array_cum_sum(k2), array_cum_sum(k4) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_cum_sum(k12), array_cum_sum(k13) FROM ${tableName} ORDER BY k1"

    def tableName2 = "tbl_test_array_range"
    sql """DROP TABLE IF EXISTS ${tableName2}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
              `k1` int(11) NULL COMMENT "",
              `k2` int(11) NULL COMMENT "",
              `k3` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName2} VALUES(-1,3,5) """
    sql """ INSERT INTO ${tableName2} VALUES(1,3,5) """
    sql """ INSERT INTO ${tableName2} VALUES(2,10,2) """
    sql """ INSERT INTO ${tableName2} VALUES(3,NULL,NULL) """
    sql """ INSERT INTO ${tableName2} VALUES(4,6,1) """
    sql """ INSERT INTO ${tableName2} VALUES(5,10,1) """
    sql """ INSERT INTO ${tableName2} VALUES(6,NULL,1) """
    sql """ INSERT INTO ${tableName2} VALUES(7,10,NULL) """
    sql """ INSERT INTO ${tableName2} VALUES(NULL,10,2) """
    sql """ INSERT INTO ${tableName2} VALUES(8,2,2) """
    sql """ INSERT INTO ${tableName2} VALUES(9,10,6) """

    qt_select "SELECT k1, array_range(k1) from ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, array_range(k1,k2) from ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, array_range(k1,k2,k3) from ${tableName2} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k2) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k3) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k4) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k5) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k6) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_compact(k7) from ${tableName} ORDER BY k1"

    qt_select "SELECT k1, array_pushfront(k2, k1) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k2, 1) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k3, 'a') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k3, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k4, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k5, 'hi') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k5, 'hi222') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k6, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k8, cast('2023-03-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k10, cast('2023-03-08 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k10, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushfront(k12, null) FROM ${tableName} ORDER BY k1"

    qt_select "SELECT k1, array_pushback(k2, k1) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k2, 1) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k3, 'a') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k3, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k4, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k5, 'hi') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k5, 'hi222') FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k6, null) from ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k8, cast('2023-03-05' as datev2)) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k10, cast('2023-03-08 10:30:00.999' as datetimev2(3))) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k10, null) FROM ${tableName} ORDER BY k1"
    qt_select "SELECT k1, array_pushback(k12, null) FROM ${tableName} ORDER BY k1"

    qt_select "select k2, bitmap_to_string(bitmap_from_array(k2)) from ${tableName} order by k1;"
    
    def tableName3 = "tbl_test_array_set"
    sql """DROP TABLE IF EXISTS ${tableName3}"""
    sql """
            create table IF NOT EXISTS ${tableName3}(
                  class_id int ,
                  class_name varchar(20),
                  student_ids array<int>
                  ) ENGINE=OLAP
                  DUPLICATE KEY(`class_id`,class_name)
                  COMMENT "OLAP"
                  DISTRIBUTED BY HASH(`class_name`) BUCKETS 2
                  PROPERTIES (
                  "replication_allocation" = "tag.location.default: 1",
                  "in_memory" = "false",
                  "storage_format" = "V2"
                  );
        """
    sql """ insert into ${tableName3} values (10005,'aaaaa',[10005,null,null]) """
    sql """ insert into ${tableName3} values (10006,'bbbbb',[60002,60002,60003,null,60005]) """
    
    qt_select_union "select class_id, student_ids, array_union(student_ids,[1,2,3]) from ${tableName3} order by class_id;"
    qt_select_union_left_const "select class_id, student_ids, array_union([1,2,3], student_ids,[1,2,3]) from ${tableName3} order by class_id;"
    qt_select_except "select class_id, student_ids, array_except(student_ids,[1,2,3]) from ${tableName3} order by class_id;"
    qt_select_except_left_const "select class_id, student_ids, array_except([1,2,3], student_ids) from ${tableName3} order by class_id;"
    qt_select_intersect "select class_id, student_ids, array_intersect(student_ids,[1,2,3,null]) from ${tableName3} order by class_id;"
    qt_select_intersect_left_const "select class_id, student_ids, array_intersect([1,2,3,null], student_ids) from ${tableName3} order by class_id;"

    def tableName4 = "tbl_test_array_datetimev2_functions"

    sql """DROP TABLE IF EXISTS ${tableName4}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName4} (
              `k1` int COMMENT "",
              `k2` ARRAY<datetimev2(3)> COMMENT "",
              `k3` ARRAY<datetimev2(3)> COMMENT "",
              `k4` ARRAY<datetimev2(6)> COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO ${tableName4} VALUES(1,
                                            ["2023-01-19 18:11:11.1111","2023-01-19 18:22:22.2222","2023-01-19 18:33:33.3333"],
                                            ["2023-01-19 18:22:22.2222","2023-01-19 18:33:33.3333","2023-01-19 18:44:44.4444"],
                                            ["2023-01-19 18:11:11.111111","2023-01-19 18:22:22.222222","2023-01-19 18:33:33.333333"]) """

    qt_select_array_datetimev2_1 "SELECT * FROM ${tableName4}"
    qt_select_array_datetimev2_2 "SELECT if(1,k2,k3) FROM ${tableName4}"
    qt_select_array_datetimev2_3 "SELECT if(0,k2,k3) FROM ${tableName4}"
    qt_select_array_datetimev2_4 "SELECT if(0,k2,k4) FROM ${tableName4}"

    // array with decimal
    sql "drop table if exists fn_test"

    sql """
            CREATE TABLE IF NOT EXISTS `fn_test` (
                `id` int null,
                `kdcmls1` decimal(9, 3) null,
                `kdcmls3` decimal(27, 9) null,
                `kdcmlv3s1` decimalv3(9, 3) null,
                `kdcmlv3s2` decimalv3(15, 5) null,
                `kdcmlv3s3` decimalv3(27, 9) null,
                `kadcml` array<decimal(27, 9)> null
            ) engine=olap
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            properties("replication_num" = "1")
        """
    sql """ insert into `fn_test` values
            (0, 0.100, 0.100000000 , 0.100, 0.10000, 0.100000000, [0.100000000, 0.100000000]),
            (1, 0.200, 0.200000000  , 0.200, 0.20000, 0.200000000, [0.200000000, 0.200000000]),
            (3, 0.300, 0.300000000  , 0.300, 0.30000, 0.300000000, [0.300000000, 0.300000000]),
            (4, 0.400, 0.400000000  , 0.400, 0.40000, 0.400000000, [0.400000000, 0.400000000]),
            (5, 0.500, 0.500000000  , 0.500, 0.50000, 0.500000000, [0.500000000, 0.500000000]),
            (6, 0.600, 0.600000000  , 0.600, 0.60000, 0.600000000, [0.600000000, 0.600000000]),
            (7, 0.700, 0.700000000  , 0.700, 0.70000, 0.700000000, [0.700000000, 0.700000000]),
            (8, 0.800, 0.800000000  , 0.800, 0.80000, 0.800000000, [0.800000000, 0.800000000]),
            (9, 0.900, 0.900000000  , 0.900, 0.90000, 0.900000000, [0.900000000, 0.900000000]),
            (10, 1.000, 1.000000000  , 1.000, 1.00000, 1.000000000, [1.000000000, 1.000000000]),
            (11, 1.100, 1.100000000  , 1.100, 1.10000, 1.100000000, [1.100000000, 1.100000000]),
            (12, 1.200, 1.200000000  , 1.200, 1.20000, 1.200000000, [1.200000000, 1.200000000]); """

    qt_sql """ select array_position(kadcml, kdcmls1), kadcml, kdcmls1 from fn_test;"""

   /*
    test scope:
    1.array_range function with datetimev2 type
    2.sequence function(alias of array_range) with int and datetimev2 type
   */
    sql "SET enable_nereids_planner=true"
    def tableName5 = "tbl_test_sequence"
    sql """drop TABLE if EXISTS ${tableName5};"""
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName5} (
                `test_id` int(11) NULL COMMENT "",
                `k1` int(11) NULL COMMENT "",
                `k2` int(11) NULL COMMENT "",
                `k3` int(11) NULL COMMENT "",
                `k4` datetimev2(0) NULL COMMENT "",
                `k5` datetimev2(3) NULL COMMENT "",
                `k6` datetimev2(6) NULL COMMENT "",              
                `step` int(11) NULL COMMENT ""              
            ) ENGINE=OLAP
            DUPLICATE KEY(`test_id`)
            DISTRIBUTED BY HASH(`test_id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
        """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(1,-1,3,5); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(2,1,3,5); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(3,2,10,2); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(4,3,NULL,NULL); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(5,4,6,1); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(6,5,10,1); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(7,6,NULL,1); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(8,7,10,NULL); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(9,NULL,10,2); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(10,8,2,2); """
    sql """ INSERT INTO ${tableName5} (test_id, k1, k2, k3) VALUES(11,9,10,6); """
    sql """ INSERT INTO ${tableName5} (test_id, k4, k5) VALUES(12, '2022-05-15 12:00:00', '2022-05-18 12:00:00.123'); """
    sql """ INSERT INTO ${tableName5} (test_id, k4, k6) VALUES(13, '2022-05-15 12:00:00', '2022-05-18 12:00:00.123456'); """
    sql """ INSERT INTO ${tableName5} (test_id, k4, k5, step) VALUES(14, '2022-05-15 12:00:00', '2022-05-18 12:00:00.123', 1); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(15, '2022-04-22 12:00:00.123', '2022-05-08 12:00:00.123456', 1); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(16, '2022-01-15 12:00:00.123', '2022-05-18 12:00:00.123456', 2); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(17, '2015-05-15 12:00:00.123', '2022-05-18 12:00:00.123456', 3); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(18, '2022-05-18 12:00:00.123', '2022-05-18 23:10:00.123456', 4); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(19, '2022-05-18 12:00:00.123', '2022-05-18 12:16:00', 5); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(20, '2022-05-18 12:00:10', '2022-05-18 12:00:30', 6); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(21, '2022-05-18 12:00:10', '2022-05-21 12:00:30', 2); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(22, '2022-05-22 12:00:10', '2022-05-25 12:00:30', 2); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(23, '2022-05-23 12:00:10', '2022-05-26 12:00:30', 2); """
    sql """ INSERT INTO ${tableName5} (test_id, k5, k6, step) VALUES(24, '2022-05-27 12:00:10', '2022-05-30 12:00:30', 2); """

    qt_table_select "SELECT k1, sequence(k1) from ${tableName5} where test_id < 12 ORDER BY test_id; "
    qt_table_select "SELECT k1, sequence(k1,k2) from ${tableName5} where test_id < 12 ORDER BY test_id; "
    qt_table_select "SELECT k1, sequence(k1,k2,k3) from ${tableName5} where test_id < 12 ORDER BY test_id; "
    qt_table_select "SELECT k4, k5, sequence(k4, k5) from ${tableName5} where test_id = 12; "
    qt_table_select "SELECT k4, k6, sequence(k4, k6) from ${tableName5} where test_id = 13; "
    qt_table_select "SELECT k4, k5, step, sequence(k4, k5, interval step day) from ${tableName5} where test_id = 14; "
    qt_table_select "SELECT k5, k6, step, sequence(k5, k6, interval step week) from ${tableName5} where test_id = 15; "
    qt_table_select "SELECT k5, k6, step, sequence(k5, k6, interval step month) from ${tableName5} where test_id = 16; "
    qt_table_select "SELECT k5, k6, step, sequence(k5, k6, interval step year) from ${tableName5} where test_id = 17; "
    qt_table_select "SELECT k5, k6, step, sequence(k5, k6, interval step hour) from ${tableName5} where test_id = 18; "
    qt_table_select "SELECT k5, k6, step, sequence(k5, k6, interval step minute) from ${tableName5} where test_id = 19; "
    qt_table_select "SELECT k5, k6, step, sequence(k5, k6, interval step second) from ${tableName5} where test_id = 20; "
    qt_table_select "SELECT k5, k6, step, sequence(k5, k6, interval step day) from ${tableName5} where test_id between 21 and 24 order by test_id; "
    qt_const_select "select array_range(cast('2022-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0))); "
    qt_const_select "select array_range(cast('2022-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0)), interval 1 day); "
    qt_const_select "select sequence(10); "
    qt_const_select "select sequence(3, 10); "
    qt_const_select "select sequence(3, 10, 2); "
    qt_const_select "select sequence(cast('2022-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0))); "
    qt_const_select "select sequence(cast('2022-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0)), interval 1 day); "
    qt_const_select "select sequence(cast('2022-05-01 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0)), interval 2 week); "
    qt_const_select "select sequence(cast('2022-01-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0)), interval 3 month); "
    qt_const_select "select sequence(cast('2019-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0)), interval 2 year); "
    qt_const_select "select sequence(cast('2022-05-18 12:00:10' as datetimev2(0)), cast('2022-05-18 22:00:30' as datetimev2(0)), interval 4 hour); "
    qt_const_select "select sequence(cast('2022-05-18 12:00:10' as datetimev2(0)), cast('2022-05-18 12:16:30' as datetimev2(0)), interval 5 minute); "
    qt_const_select "select sequence(cast('2022-05-18 12:00:10' as datetimev2(0)), cast('2022-05-18 12:00:30' as datetimev2(0)), interval 6 second); "
    qt_const_select "select sequence(cast('2022-05-18 12:00:10' as datetimev2(0)), cast('2022-05-18 22:00:30' as datetimev2(0)), interval -4 hour); "
    qt_const_select "select sequence(cast('2022-35-38 12:00:10' as datetimev2(0)), cast('2022-05-18 22:00:30' as datetimev2(0)), interval 4 hour); "
    qt_const_select "select sequence(cast('2022-05-15 12:00:00' as datetimev2(0)), cast('2022-35-37 12:00:00' as datetimev2(0))); "
    qt_const_select "select sequence(1, 10, interval 10 day); "
    qt_const_select "select sequence(cast('2022-35-38 12:00:10' as datetimev2(0)), cast('2022-05-18 22:00:30' as datetimev2(0))); "
    qt_const_select "select sequence(1, 10, 0); "
    qt_const_select "select sequence(cast('2022-05-15 12:00:00' as datetimev2(0)), cast('2022-05-17 12:00:00' as datetimev2(0)), interval 0 day); "
}
