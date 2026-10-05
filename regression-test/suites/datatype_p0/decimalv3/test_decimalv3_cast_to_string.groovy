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

suite("test_decimalv3_cast_to_string") {
    sql """
        drop table if exists decimalv3_cast_to_string_test;
    """
    sql """
        create table decimalv3_cast_to_string_test (k1 decimalv3(38,6)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into decimalv3_cast_to_string_test values (null), (0), (1), (1.123), (0.1), (0.000001), (99999999999999999999999999999999), ("99999999999999999999999999999999.000001"), ("99999999999999999999999999999999.999999");
    """
    sql """
        insert into decimalv3_cast_to_string_test values (-1), (-1.123), (-0.1), (-0.000001), (-99999999999999999999999999999999), ("-99999999999999999999999999999999.000001"), ("-99999999999999999999999999999999.999999");
    """

    qt_cast1 """
        select k1, cast(k1 as varchar) from decimalv3_cast_to_string_test order by 1;
    """

    sql """
        drop table if exists decimalv3_cast_to_string_join_test_l;
    """
    sql """
        drop table if exists decimalv3_cast_to_string_join_test_r;
    """
    sql """
        create table decimalv3_cast_to_string_join_test_l(k1 decimalv3(10, 3)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        create table decimalv3_cast_to_string_join_test_r(kk1 decimalv3(10, 4)) distributed by hash(kk1) properties("replication_num"="1");
    """
    sql """
        insert into decimalv3_cast_to_string_join_test_l values (1.123);
    """
    sql """
        insert into decimalv3_cast_to_string_join_test_r values (1.123);
    """
    qt_join1 """
        select * from decimalv3_cast_to_string_join_test_l, decimalv3_cast_to_string_join_test_r where k1 = kk1 order by 1, 2;
    """
    // scale is different, cast result will not equal
    qt_cast_join1 """
        select k1, cast(k1 as char(16)) k1cast, kk1, cast(kk1 as char(16)) kk1cast
            from decimalv3_cast_to_string_join_test_l, decimalv3_cast_to_string_join_test_r
        where cast(k1 as char(16)) = cast(kk1 as char(16)) order by 1, 2;
    """
    qt_cast_join2 """
        select k1, cast(k1 as varchar) k1cast, kk1, cast(kk1 as varchar) kk1cast
            from decimalv3_cast_to_string_join_test_l, decimalv3_cast_to_string_join_test_r
        where cast(k1 as varchar) = cast(kk1 as varchar) order by 1, 2;
    """
}