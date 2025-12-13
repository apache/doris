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

suite("test_hamming_distance") {
    // this table has nothing todo. just make it eaiser to generate query
    sql " drop table if exists hits_hamming_distance "
    sql """ create table hits_hamming_distance(
                nothing boolean
            )
            properties("replication_num" = "1");
    """
    sql "insert into hits_hamming_distance values(true);"

    sql " drop table if exists arg1_hamming_distance"
    sql """
        create table arg1_hamming_distance (
            k0 int,
            a varchar(100) not null,
            b varchar(100) null,
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select hamming_distance(b, b) from arg1_hamming_distance"
    order_qt_empty_not_nullable "select hamming_distance(a, a) from arg1_hamming_distance"
    order_qt_empty_partial_nullable "select hamming_distance(a, b) from arg1_hamming_distance"

    sql "insert into arg1_hamming_distance values (1, 'abc', null), (2, 'hello', null), (3, 'test', null)"
    order_qt_all_null "select hamming_distance(b, b) from arg1_hamming_distance"

    sql "truncate table arg1_hamming_distance"
    sql """ insert into arg1_hamming_distance values 
        (1, 'abc', 'abc'), 
        (2, 'abc', 'axc'), 
        (3, 'abc', 'xyz'), 
        (4, 'hello', 'hallo'), 
        (5, 'test', 'text'), 
        (6, '', ''), 
        (7, 'a', 'a'), 
        (8, 'abc', 'abcd'),
        (9, 'abc', 'ab'),
        (10, 'hello', 'hi'),
        (11, 'a', ''),
        (12, '', 'a'),
        (13, 'abc', null),
        (14, null, 'abc'),
        (15, null, null);
    """

    /// all values
    order_qt_nullable """
        SELECT hamming_distance(t.arg1_hamming_distance, t.ARG2) as result
        FROM (
            SELECT hits_hamming_distance.nothing, TABLE1.arg1_hamming_distance, TABLE1.order1, TABLE2.ARG2, TABLE2.order2
            FROM hits_hamming_distance
            CROSS JOIN (
                SELECT b as arg1_hamming_distance, k0 as order1
                FROM arg1_hamming_distance
            ) as TABLE1
            CROSS JOIN (
                SELECT b as ARG2, k0 as order2
                FROM arg1_hamming_distance
            ) as TABLE2
        )t;
    """

    /// nullables
    order_qt_not_nullable "select hamming_distance(a, a) from arg1_hamming_distance"
    order_qt_partial_nullable "select hamming_distance(a, b) from arg1_hamming_distance"
    order_qt_nullable_no_null "select hamming_distance(a, nullable(a)) from arg1_hamming_distance"

    /// consts. most by BE-UT
    order_qt_const_nullable "select hamming_distance(NULL, NULL) from arg1_hamming_distance"
    order_qt_partial_const_nullable "select hamming_distance(NULL, b) from arg1_hamming_distance"
    order_qt_const_not_nullable "select hamming_distance('abc', 'abc') from arg1_hamming_distance"
    order_qt_const_other_nullable "select hamming_distance('abc', b) from arg1_hamming_distance"
    order_qt_const_other_not_nullable "select hamming_distance(a, 'abc') from arg1_hamming_distance"
    order_qt_const_nullable_no_null "select hamming_distance(nullable('abc'), nullable('abc'))"
    order_qt_const_nullable_no_null_multirows "select hamming_distance(nullable('abc'), nullable('abc'))"
    order_qt_const_partial_nullable_no_null "select hamming_distance('abc', nullable('abc'))"

    /// Test same length strings
    order_qt_const_same_length "select hamming_distance('abc', 'abc') from arg1_hamming_distance"
    order_qt_const_same_length_diff "select hamming_distance('abc', 'axc') from arg1_hamming_distance"
    order_qt_const_same_length_all_diff "select hamming_distance('abc', 'xyz') from arg1_hamming_distance"
    
    /// Test different length strings (should return NULL)
    order_qt_const_diff_length "select hamming_distance('abc', 'abcd') from arg1_hamming_distance"
    order_qt_const_diff_length2 "select hamming_distance('abc', 'ab') from arg1_hamming_distance"
    order_qt_const_diff_length_empty "select hamming_distance('', 'a') from arg1_hamming_distance"
    order_qt_const_diff_length_empty2 "select hamming_distance('a', '') from arg1_hamming_distance"

    /// folding
    check_fold_consistency "hamming_distance('abc', 'abc')"
    check_fold_consistency "hamming_distance('abc', 'axc')"
    check_fold_consistency "hamming_distance('', '')"
    check_fold_consistency "hamming_distance('hello', 'hallo')"
}

