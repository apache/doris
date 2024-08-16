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

suite("test_assert_true") {
    sql " drop table if exists assert_true_not_null "
    sql """
        create table assert_true_not_null(
            k0 boolean not null,
            k1 int
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql " insert into assert_true_not_null values (true, 1);"
    qt_sql1 """ select assert_true(k0, cast(123 as varchar)) from assert_true_not_null; """
    qt_sql2 """ select assert_true(k0, "nn123") from assert_true_not_null; """

    sql " insert into assert_true_not_null values (false, 2);"
    test {
        sql """ select assert_true(k0, cast(123 as varchar)) from assert_true_not_null; """
        exception "123"
    }
    test {
        sql """ select assert_true(k0, "nn123") from assert_true_not_null; """
        exception "nn123"
    }
    test {
        sql """ select assert_true(1, k1) from assert_true_not_null; """
        exception "assert_true only accept constant for 2nd argument"
    }



    sql " drop table if exists assert_true_null "
    sql """
        create table assert_true_null(
            k0 boolean null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql " insert into assert_true_null values (true), (true), (true);"
    qt_sql3 """ select assert_true(k0, cast(123 as varchar)) from assert_true_null; """
    qt_sql4 """ select assert_true(k0, "nn123") from assert_true_null; """

    sql " insert into assert_true_null values (null);"
    test {
        sql """ select assert_true(k0, cast(123 as varchar)) from assert_true_null; """
        exception "123"
    }
    test {
        sql """ select assert_true(k0, "nn123") from assert_true_null; """
        exception "nn123"
    }

    qt_sql """ select assert_true(1, "wrong"); """
    test {
        sql """ select assert_true(0, nullable("wrong")); """
        exception "assert_true only accept constant for 2nd argument"
    }
    test {
        sql """ select assert_true(0, "wrong"); """
        exception "wrong"
    }
}
