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

suite("test_insert_strict_mode_and_filter_ratio_custom","p0") {
    multi_sql """
        drop table if exists test_src;
        drop table if exists test_dst;
        drop table if exists test_dst_not_null;
        drop table if exists test_dst_int;
        drop table if exists test_dst_int_not_null;

        create table test_src(f1 int, f2 char(6)) properties('replication_num' = '1');
        create table test_dst(f1 int, f2 char(3)) properties('replication_num' = '1');
        create table test_dst_not_null(f1 int not null, f2 char(3) not null) properties('replication_num' = '1');
        create table test_dst_int(f1 int, f2 int) properties('replication_num' = '1');
        create table test_dst_int_not_null(f1 int not null, f2 int not null) properties('replication_num' = '1');
    """
    def enableAutoSubStringForInsert = {
        multi_sql """
            set enable_insert_value_auto_cast = false;
            set enable_strict_cast = false;
        """
    }
    sql """
        set insert_max_filter_ratio = 1;
    """

    // For insert statements, if enable_insert_value_auto_cast = true && enable_strict_cast = false
    // FE will plan substring for string value whose length is longer than schema,
    // and insert truncated string, no matter enable_insert_strict is true or false.

    // test 1: INSERT INTO VALUES, str length is longer than schema, enable_insert_strict=true.
    sql """
        set enable_insert_strict=true;
     """
    // test 1.1: insert strict, NO auto cast, NO strict cast, plan no substr, should failed
    sql """
        set enable_insert_value_auto_cast = false;
    """
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set enable_strict_cast = false;
    """
    test {
        sql """
        insert into test_dst values(1, '123456');
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")

    // test 1.2: insert strict, NO auto cast, WITH strict cast, plan no substr, should failed
    multi_sql """
        set enable_strict_cast = true;
    """
    test {
        sql """
            insert into test_dst values(1, '123456');
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")

    // test 1.3: insert strict, WITH auto cast, no strict cast, plan substr, insert truncated str
    sql """
        set enable_insert_value_auto_cast = true;
    """
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set enable_strict_cast = false;
        insert into test_dst values(1, '123456');
    """
    order_qt_insert_values_str_long_than_schema0 """
        select * from test_dst;
    """
    // test 1.4: insert strict, WITH auto cast, WITH strict cast, plan NO substr, should fail
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set enable_strict_cast = true;
    """
    test {
        sql """
        insert into test_dst values(1, '123456');
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")
    // end test 1: insert into values, str length is longer than schema, enable_insert_strict=true.

    // test 2: INSERT INTO VALUES, str length is longer than schema, enable_insert_strict=false
    // insert_max_filter_ratio affect whether insert succeed or fail
    sql """
        set enable_insert_strict=false;
    """
    // test 2.1: NOT insert strict, NO auto cast, NO strict cast, plan no substr
    multi_sql """
        set enable_insert_value_auto_cast = false;
        set enable_strict_cast = false;
    """
    // insert_max_filter_ratio = 0, fail
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set insert_max_filter_ratio = 0;
    """
    test {
        sql """
        insert into test_dst values(1, '123456');
        """
        exception """Insert has too many filtered data"""
    }
    testExpectNoResult("select * from test_dst;")
    // insert_max_filter_ratio = 1, OK, no data is inserted
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set insert_max_filter_ratio = 1;
    """
    sql """
        insert into test_dst values(1, '123456');
    """
    testExpectNoResult("select * from test_dst;")
    // test 2.2: NOT insert strict, NO auto cast, WITH strict cast, plan no substr
    sql """
        set enable_strict_cast = true;
    """
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set insert_max_filter_ratio = 0.5;
    """
    test {
        sql """
            insert into test_dst values(1, '123456');
        """
        exception """Insert has too many filtered data"""
    }
    testExpectNoResult("select * from test_dst;")

    multi_sql """
        set insert_max_filter_ratio = 1;
    """
    sql """
        insert into test_dst values(1, '123456');
    """
    testExpectNoResult("select * from test_dst;")
    // test 2.3: NOT insert strict, WITH auto cast, NO strict cast, plan substr, insert OK
    sql """
        set enable_insert_value_auto_cast = true;
    """
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 0;
    """
    sql """
        insert into test_dst values(1, '123456');
    """
    order_qt_insert_values_str_long_than_schema6 """
        select * from test_dst;
    """

    // test 2.4: NOT insert strict, WITH auto cast, WITH strict cast, plan no substr
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        set enable_strict_cast = true;
        set insert_max_filter_ratio = 0.5;
    """
    test {
        sql """
            insert into test_dst values(1, '123456');
        """
        exception """Insert has too many filtered data"""
    }
    testExpectNoResult("select * from test_dst;")
    // insert_max_filter_ratio = 1;
    multi_sql """
        set insert_max_filter_ratio = 1;
        insert into test_dst values(1, '123456');
    """
    testExpectNoResult("select * from test_dst;")
    // END test 2: insert into values, str length is longer than schema, enable_insert_strict=false,

    // test 3: INSERT INTO SELECT, str length is longer than schema, enable_insert_strict=true
    multi_sql """
        set enable_insert_strict=true;
        set insert_max_filter_ratio = 1;
    """
    multi_sql """
        truncate table test_src;
        truncate table test_dst;
        insert into test_src values(1, '123456');
    """
    // test 3.1: insert strict ON, auto cast OFF, strict cast OFF, plan no substr, should failed
    multi_sql """
        truncate table test_dst;
        set enable_insert_value_auto_cast = false;
        set enable_strict_cast = false;
    """
    test {
        sql """
        insert into test_dst select * from test_src;
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")
    
    // test 3.2: insert strict ON, auto cast OFF, strict cast ON, plan no substr, should failed
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = true;
    """
    test {
        sql """
            insert into test_dst select * from test_src;
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")

    // test 3.3: insert strict ON, auto cast ON,  strict cast OFF, plan substr, insert truncated str
    sql """
        set enable_insert_value_auto_cast = true;
    """
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = false;
        insert into test_dst select * from test_src;
    """
    order_qt_insert_select_str_long_than_schema2 """
        select * from test_dst;
    """
    // test 3.4: insert strict ON, auto cast ON, strict cast ON, plan NO substr, should fail
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = true;
    """
    test {
        sql """
        insert into test_dst select * from test_src;
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")
    // END test 3: insert into select, str length is longer than schema, enable_insert_strict=true

    // test 4: INSERT INTO SELECT, str length is longer than schema, enable_insert_strict=false
    // test 4.5: insert strict OFF, auto cast OFF, strict cast OFF, plan no substr, should failed
    multi_sql """
        set enable_insert_strict=false;
        set enable_insert_value_auto_cast = false;
    """
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 0.5;
    """
    test {
        sql """
        insert into test_dst select * from test_src;
        """
        exception """Insert has too many filtered data"""
    }
    testExpectNoResult("select * from test_dst;")

    // insert_max_filter_ratio = 1, should be OK, no data is inserted
    sql """
        set insert_max_filter_ratio = 1;
    """
    sql """
    insert into test_dst select * from test_src;
    """
    testExpectNoResult("select * from test_dst;")
    
    // test 4.2: insert strict OFF, auto cast OFF, strict cast ON, plan no substr, should failed
    multi_sql """
        set enable_strict_cast = true;
        set insert_max_filter_ratio = 0.5;
    """
    test {
        sql """
            insert into test_dst select * from test_src;
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")

    // insert_max_filter_ratio = 1, should be OK, no data is inserted
    sql """
        set insert_max_filter_ratio = 1;
    """
    sql """
    insert into test_dst select * from test_src;
    """
    testExpectNoResult("select * from test_dst;")

    // test 4.3: insert strict OFF, auto cast ON, strict cast OFF, plan substr, insert truncated str
    sql """
        set enable_insert_value_auto_cast = true;
        set insert_max_filter_ratio = 0;
    """
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = false;
        insert into test_dst select * from test_src;
    """
    order_qt_insert_select_str_long_than_schema2 """
        select * from test_dst;
    """
    // test 4.4: insert strict OFF, auto cast ON, strict cast ON, plan NO substr, should fail
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = true;
        set insert_max_filter_ratio = 0.5;
    """
    test {
        sql """
            insert into test_dst select * from test_src;
        """
        exception """the length of input is too long than schema"""
    }
    testExpectNoResult("select * from test_dst;")

    multi_sql """
        set insert_max_filter_ratio = 1;
    """
    sql """
        insert into test_dst select * from test_src;
    """
    testExpectNoResult("select * from test_dst;")
    // END test 4: insert into select, str length is longer than schema, enable_insert_strict=true

    // test 5: insert into values, invalid string to number, enable_insert_strict=true
    multi_sql """
        set enable_insert_strict=true;
    """
    // test 5.1: insert strict ON, strict cast OFF, cast to NULL, NULL is inserted
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 0;
    """
    sql """
        insert into test_dst values('NA', '123'), ("1", "456");
    """
    order_qt_insert_values_invalid_str_to_number0 """
        select * from test_dst;
    """

    // test 5.2: insert strict ON, strict cast ON
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = true;
        set insert_max_filter_ratio = 1;
    """
    test {
        sql """
            insert into test_dst values('NA', '123'), ("1", "456");
        """
        exception """can't cast"""
    }
    testExpectNoResult("select * from test_dst;")

    // test 5.3: insert strict ON, strict cast OFF, insert into not nullable, cast to NULL, should fail even insert_max_filter_ratio = 1, because NULL can't be inserted into not nullable column
    multi_sql """
        truncate table test_dst_not_null;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 1;
    """
    test {
        sql """
            insert into test_dst_not_null values('NA', '123'), ("1", "456");
        """
        exception """null value for not null column"""
    }
    testExpectNoResult("select * from test_dst_not_null;")
    // END test 5: insert into values, invalid string to number, enable_insert_strict=true

    // test 6: insert into values, invalid string to number, enable_insert_strict=false
    multi_sql """
        set enable_insert_strict=false;
    """
    // test 6.1: insert strict OFF, strict cast OFF, cast to NULL, NULL is inserted
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 0;
    """
    sql """
        insert into test_dst values('NA', '123'), ("1", "456");
    """
    order_qt_insert_values_invalid_str_to_number1 """
        select * from test_dst;
    """
    // test 6.2: insert strict OFF, strict cast ON, cast fail
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = true;
        set insert_max_filter_ratio = 1;
    """
    test {
        sql """
            insert into test_dst values('NA', '123'), ("1", "456");
        """
        exception """can't cast"""
    }
    testExpectNoResult("select * from test_dst;")
    // END test 6: insert into values, invalid string to number, enable_insert_strict=false

    // test 7: insert into select, invalid string to number, enable_insert_strict=true
    multi_sql """
        truncate table test_src;
        truncate table test_dst_int;
        set enable_insert_strict=true;
        insert into test_src values(0, 'NA'), (1, '1');
    """
    // test 7.1: insert strict ON, strict cast OFF, cast to NULL, NULL is inserted
    multi_sql """
        truncate table test_dst_int;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 0;
    """
    sql """
        insert into test_dst_int select * from test_src;
    """
    order_qt_insert_select_invalid_str_to_number0 """
        select * from test_dst_int;
    """
    // test 7.2: insert strict ON, strict cast ON
    multi_sql """
        truncate table test_dst_int;
        set enable_strict_cast = true;
        set insert_max_filter_ratio = 1;
    """
    test {
        sql """
            insert into test_dst_int select * from test_src;
        """
        exception """parse number fail"""
    }
    testExpectNoResult("select * from test_dst_int;")

    // test 7.3: insert strict ON, strict cast OFF, insert into not nullable, cast to NULL, should fail even insert_max_filter_ratio = 1, because NULL can't be inserted into not nullable column
    multi_sql """
        truncate table test_dst_int_not_null;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 1;
    """
    test {
        sql """
            insert into test_dst_int_not_null select * from test_src;
        """
        exception """null value for not null column"""
    }
    testExpectNoResult("select * from test_dst_int_not_null;")
    // END test 7: insert into select, invalid string to number, enable_insert_strict=true

    // test 8: insert into select, invalid string to number, enable_insert_strict=false
    multi_sql """
        truncate table test_src;
        truncate table test_dst_int;
        set enable_insert_strict=false;
        insert into test_src values(0, 'NA'), (1, '1');
    """
    // test 8.1: insert strict OFF, strict cast OFF, cast to NULL, NULL is inserted
    multi_sql """
        truncate table test_dst;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 0;
    """
    sql """
        insert into test_dst_int select * from test_src;
    """
    order_qt_insert_select_invalid_str_to_number1 """
        select * from test_dst_int;
    """
    // test 8.2: insert strict OFF, strict cast ON, cast fail
    multi_sql """
        truncate table test_dst_int;
        set enable_strict_cast = true;
        set insert_max_filter_ratio = 1;
    """
    test {
        sql """
            insert into test_dst_int select * from test_src;
        """
        exception """parse number fail"""
    }
    testExpectNoResult("select * from test_dst_int;")
    // test 8.3: insert strict OFF, strict cast OFF, insert into not nullable, cast to NULL, should fail even insert_max_filter_ratio = 1, because NULL can't be inserted into not nullable column
    multi_sql """
        truncate table test_dst_int_not_null;
        set enable_strict_cast = false;
        set insert_max_filter_ratio = 0.4;
    """
    test {
        sql """
            insert into test_dst_int_not_null select * from test_src;
        """
        exception """null value for not null column"""
    }
    testExpectNoResult("select * from test_dst_int_not_null;")

    multi_sql """
        set insert_max_filter_ratio = 1;
    """
    sql """
        insert into test_dst_int_not_null select * from test_src;
    """
    order_qt_insert_select_invalid_str_to_number2 """
        select * from test_dst_int_not_null;
    """
    // END test 8: insert into select, invalid string to number, enable_insert_strict=false

    // test 9: str == int, invalid string to number, enable_insert_strict=true
    multi_sql """
        truncate table test_src;
        truncate table test_dst_int;
        set insert_max_filter_ratio = 0;
        insert into test_src values(0, 'NA'), (0, '1');
    """
    def testInvalidStrCmpInt = { bEnableInsertStrict ->
        // test 9.1: strict cast ON, cast fail
        multi_sql """
            truncate table test_dst_int;
            set enable_insert_strict = ${bEnableInsertStrict};
            set enable_strict_cast = true;
            set insert_max_filter_ratio = 1;
        """
        test {
            sql """
                insert into test_dst_int select f1, sum(if(f2 = 1, f2, -9999)) from test_src group by f1;
            """
            exception """parse number fail"""
        }
        testExpectNoResult("select * from test_dst_int;")

        // test 9.2: strict cast OFF, should be OK, invalid string is cast to NULL, sum result is -9998
        multi_sql """
            set enable_strict_cast = false;
        """
        sql """
            insert into test_dst_int select f1, sum(if(f2 = 1, f2, -9999)) from test_src group by f1;
        """
        order_qt_insert_select_invalid_str_cmp_int """
            select * from test_dst_int;
        """
    }
    testInvalidStrCmpInt(true)
    testInvalidStrCmpInt(false)
    // END test 9: str == int, invalid string to number
}

