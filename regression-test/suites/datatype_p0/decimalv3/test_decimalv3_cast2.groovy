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

suite("test_decimalv3_cast2") {
    // test cast integers and float numbers to decimal

    // cast int to decimal
    def prepare_test_int_to_decimal32_1 = {
        sql "drop table if exists test_int_to_decimal32_1;"
        // 10.0
        sql """
            CREATE TABLE test_int_to_decimal32_1(
              k1 int
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_int_to_decimal32_1()
    sql """
        insert into test_int_to_decimal32_1 values(12);
    """
    // to decimal32
    test {
        sql """
            select cast(k1 as decimalv3(1, 0)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(1, 1)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int_to_decimal_1 """
        select cast(k1 as decimalv3(2, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_4 """
        select cast(k1 as decimalv3(9, 7)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 8)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 9)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal64
    qt_int_to_decimal_5 """
        select cast(k1 as decimalv3(10, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_6 """
        select cast(k1 as decimalv3(10, 8)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(10, 9)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int_to_decimal_7 """
        select cast(k1 as decimalv3(18, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_8 """
        select cast(k1 as decimalv3(18, 16)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(18, 17)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 18)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal128
    qt_int_to_decimal_11 """
        select cast(k1 as decimalv3(38, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_12 """
        select cast(k1 as decimalv3(38, 36)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(38, 37)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 38)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to deciml256
    sql "set enable_decimal256=true;"
    qt_int_to_decimal_15 """
        select cast(k1 as decimalv3(76, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_16 """
        select cast(k1 as decimalv3(76, 74)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(76, 75)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 76)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"
    prepare_test_int_to_decimal32_1()
    sql """
        insert into test_int_to_decimal32_1 values(0);
    """
    qt_int_to_decimal_17 """
        select cast(k1 as decimalv3(1, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_18 """
        select cast(k1 as decimalv3(1, 1)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_19 """
        select cast(k1 as decimalv3(9, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_20 """
        select cast(k1 as decimalv3(9, 1)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_21 """
        select cast(k1 as decimalv3(9, 9)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_22 """
        select cast(k1 as decimalv3(18, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_23 """
        select cast(k1 as decimalv3(18, 18)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_24 """
        select cast(k1 as decimalv3(38, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_25 """
        select cast(k1 as decimalv3(38, 38)) from test_int_to_decimal32_1;
    """
    sql "set enable_decimal256=true;"
    qt_int_to_decimal_26 """
        select cast(k1 as decimalv3(76, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_27 """
        select cast(k1 as decimalv3(76, 76)) from test_int_to_decimal32_1;
    """
    sql "set enable_decimal256=false;"

    prepare_test_int_to_decimal32_1()
    // max int
    sql """
        insert into test_int_to_decimal32_1 values(2147483647);
    """
    test {
        sql """
            select cast(k1 as decimalv3(1, 0)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(1, 1)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(9, 0)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(9, 1)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 9)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal64
    qt_int_to_decimal_t2_1 """
        select cast(k1 as decimalv3(10, 0)) from test_int_to_decimal32_1;
    """
    test {
        sql """
        select cast(k1 as decimalv3(10, 1)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(10, 10)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int_to_decimal_t2_2 """
        select cast(k1 as decimalv3(18, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_t2_3 """
        select cast(k1 as decimalv3(18, 1)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_t2_4 """
        select cast(k1 as decimalv3(18, 8)) from test_int_to_decimal32_1;
    """
    test {
        sql """
        select cast(k1 as decimalv3(18, 9)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 17)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 18)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal128
    qt_int_to_decimal_t2_7 """
        select cast(k1 as decimalv3(38, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_t2_8 """
        select cast(k1 as decimalv3(38, 28)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(38, 37)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 38)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to deciml256
    sql "set enable_decimal256=true;"
    qt_int_to_decimal_t2_11 """
        select cast(k1 as decimalv3(76, 0)) from test_int_to_decimal32_1;
    """
    qt_int_to_decimal_t2_12 """
        select cast(k1 as decimalv3(76, 66)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(76, 75)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 76)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"


    // multipy result can't overflow, may narrow integral
    prepare_test_int_to_decimal32_1()
    sql """
        insert into test_int_to_decimal32_1 values(12345678);
    """
    // actural not narrow integral
    qt_int_to_decimal_19 """
        select cast(k1 as decimalv3(10, 2)) from test_int_to_decimal32_1;
    """

    prepare_test_int_to_decimal32_1()
    sql """
        insert into test_int_to_decimal32_1 values(922337203);
    """
    // multiply result not overflow, but final cast result overflow
    test {
        sql """
            select cast(k1 as decimalv3(18, 10)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }

    prepare_test_int_to_decimal32_1()
    sql """
        insert into test_int_to_decimal32_1 values(922337204);
    """
    test {
        sql """
            select cast(k1 as decimalv3(18, 10)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }

    // multiply not overflow
    qt_int_to_decimal_t2_20 """
        select cast(k1 as decimalv3(19, 9)) from test_int_to_decimal32_1;
    """

    // multiply not overflow, narrow integer
    test {
        sql """
        select cast(k1 as decimalv3(19, 11)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    sql "drop table test_int_to_decimal32_1;"

    // test negative int32 
    // cast negative int to decimal
    prepare_test_int_to_decimal32_1()
    sql """
        insert into test_int_to_decimal32_1 values(-12);
    """
    // to decimal32
    test {
        sql """
            select cast(k1 as decimalv3(1, 0)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(1, 1)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_negative_int_to_decimal_1 """
        select cast(k1 as decimalv3(2, 0)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_2 """
        select cast(k1 as decimalv3(3, 1)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_3 """
        select cast(k1 as decimalv3(9, 1)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_4 """
        select cast(k1 as decimalv3(9, 7)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 8)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 9)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal64
    qt_negative_int_to_decimal_5 """
        select cast(k1 as decimalv3(10, 0)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_6 """
        select cast(k1 as decimalv3(10, 8)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(10, 9)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_negative_int_to_decimal_7 """
        select cast(k1 as decimalv3(18, 0)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_8 """
        select cast(k1 as decimalv3(18, 16)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(18, 17)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 18)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal128
    qt_negative_int_to_decimal_9 """
        select cast(k1 as decimalv3(19, 0)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_10 """
        select cast(k1 as decimalv3(19, 17)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(19, 18)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(19, 19)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_negative_int_to_decimal_11 """
        select cast(k1 as decimalv3(38, 0)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_12 """
        select cast(k1 as decimalv3(38, 36)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(38, 37)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 38)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    // to deciml256
    sql "set enable_decimal256=true;"
    qt_negative_int_to_decimal_13 """
        select cast(k1 as decimalv3(39, 0)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_14 """
        select cast(k1 as decimalv3(39, 37)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(39, 38)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(39, 39)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_negative_int_to_decimal_15 """
        select cast(k1 as decimalv3(76, 0)) from test_int_to_decimal32_1;
    """
    qt_negative_int_to_decimal_16 """
        select cast(k1 as decimalv3(76, 74)) from test_int_to_decimal32_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(76, 75)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 76)) from test_int_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"
    // end test negative int32 
    
    // cast largint to decimal
    def prepare_test_int128_to_decimal_1 = {
        sql "drop table if exists test_int128_to_decimal_1;"
        // 39.0
        sql """
            CREATE TABLE test_int128_to_decimal_1(
              k1 largeint
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_int128_to_decimal_1()
    sql """
        insert into test_int128_to_decimal_1 values(12);
    """
    // to decimal32
    test {
        sql """
            select cast(k1 as decimalv3(1, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(1, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int128_to_decimal_1 """
        select cast(k1 as decimalv3(2, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_2 """
        select cast(k1 as decimalv3(3, 1)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_3 """
        select cast(k1 as decimalv3(9, 1)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_4 """
        select cast(k1 as decimalv3(9, 7)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 8)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 9)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal64
    qt_int128_to_decimal_5 """
        select cast(k1 as decimalv3(10, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_6 """
        select cast(k1 as decimalv3(10, 8)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(10, 9)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int128_to_decimal_7 """
        select cast(k1 as decimalv3(18, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_8 """
        select cast(k1 as decimalv3(18, 16)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(18, 17)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 18)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal128
    qt_int128_to_decimal_9 """
        select cast(k1 as decimalv3(19, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_10 """
        select cast(k1 as decimalv3(19, 17)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(19, 18)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(19, 19)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int128_to_decimal_11 """
        select cast(k1 as decimalv3(38, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_12 """
        select cast(k1 as decimalv3(38, 36)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(38, 37)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 38)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to deciml256
    sql "set enable_decimal256=true;"
    qt_int128_to_decimal_13 """
        select cast(k1 as decimalv3(39, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_14 """
        select cast(k1 as decimalv3(39, 37)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(39, 38)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(39, 39)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int128_to_decimal_15 """
        select cast(k1 as decimalv3(76, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_16 """
        select cast(k1 as decimalv3(76, 74)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(76, 75)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 76)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"

    // max int128
    prepare_test_int128_to_decimal_1()
    sql """
        insert into test_int128_to_decimal_1 values(170141183460469231731687303715884105727);
    """
    // to decimal32
    test {
        sql """
            select cast(k1 as decimalv3(1, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(1, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(2, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(3, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(9, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 8)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 9)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal64
    test {
        sql """
        select cast(k1 as decimalv3(10, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(10, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(10, 10)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(18, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 18)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal128
    test {
        sql """
        select cast(k1 as decimalv3(19, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(19, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(19, 19)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(38, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(38, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 38)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to deciml256
    sql "set enable_decimal256=true;"
    qt_int128_to_decimal_t2_1 """
        select cast(k1 as decimalv3(39, 0)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(39, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(39, 39)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    qt_int128_to_decimal_t2_2 """
        select cast(k1 as decimalv3(76, 0)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_t2_3 """
        select cast(k1 as decimalv3(76, 1)) from test_int128_to_decimal_1;
    """
    qt_int128_to_decimal_t2_4 """
        select cast(k1 as decimalv3(76, 37)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(76, 38)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 39)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 76)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"


    // test negative int128 to decimal
    // min int128
    prepare_test_int128_to_decimal_1()
    sql """
        insert into test_int128_to_decimal_1 values(-170141183460469231731687303715884105728);
    """
    // to decimal32
    test {
        sql """
            select cast(k1 as decimalv3(1, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(1, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(9, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(9, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 9)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal64
    test {
        sql """
        select cast(k1 as decimalv3(18, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 18)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal128
    test {
        sql """
        select cast(k1 as decimalv3(38, 0)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(38, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 38)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    // to deciml256
    sql "set enable_decimal256=true;"
    qt_negative_int128_to_decimal_1 """
        select cast(k1 as decimalv3(39, 0)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(39, 1)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(39, 39)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    qt_negative_int128_to_decimal_2 """
        select cast(k1 as decimalv3(76, 0)) from test_int128_to_decimal_1;
    """
    qt_negative_int128_to_decimal_3 """
        select cast(k1 as decimalv3(76, 1)) from test_int128_to_decimal_1;
    """
    qt_negative_int128_to_decimal_4 """
        select cast(k1 as decimalv3(76, 37)) from test_int128_to_decimal_1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(76, 38)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 39)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(76, 76)) from test_int128_to_decimal_1;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"


    // cast float to decimal
    def prepare_test_float_to_decimal32_1 = {
        sql "drop table if exists test_float_to_decimal32_1;"
        sql """
            CREATE TABLE test_float_to_decimal32_1(
              k1 int,
              k2 float
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_float_to_decimal32_1()
    sql """
        insert into test_float_to_decimal32_1 values(1, 123456789.523);
    """

    test {
        sql """
            select cast(k2 as decimalv3(9,1)) from test_float_to_decimal32_1;
        """
        exception "overflow"
    }
    qt_castfloat_to_decimal32_1 """
        select cast(k2 as decimalv3(9,0)) from test_float_to_decimal32_1;
    """
    qt_castfloat_to_decimal32_2 """
        select cast(k2 as decimalv3(10,1)) from test_float_to_decimal32_1;
    """
    qt_castfloat_to_decimal32_3 """
        select cast(k2 as decimalv3(12,3)) from test_float_to_decimal32_1;
    """
    qt_castfloat_to_decimal32_4 """
        select cast(k2 as decimalv3(18,6)) from test_float_to_decimal32_1;
    """
    test {
        sql """
            select cast(k2 as decimalv3(38,30)) from test_float_to_decimal32_1;
        """
        exception "overflow"
    }
    qt_castfloat_to_decimal32_5 """
        select cast(k2 as decimalv3(38,29)) from test_float_to_decimal32_1;
    """
    qt_castfloat_to_decimal32_6 """
        select cast(k2 as decimalv3(38,26)) from test_float_to_decimal32_1;
    """
    sql "set enable_decimal256=true;"
    test {
        sql """
            select cast(k2 as decimalv3(76,68)) from test_float_to_decimal32_1;
        """
        exception "overflow"
    }
    /*
    float numbers i not accurate:
    value: 123456792
    out: inf
    */
    // qt_castfloat_to_decimal32_7 """
    //     select cast(k2 as decimalv3(76,67)) from test_float_to_decimal32_1;
    // """
    // qt_castfloat_to_decimal32_8 """
    //     select cast(k2 as decimalv3(76,64)) from test_float_to_decimal32_1;
    // """
    sql "set enable_decimal256=false;"

    prepare_test_float_to_decimal32_1()
    sql """
        insert into test_float_to_decimal32_1 values(1, 1000000000.001);
    """
    test {
        sql """
            select cast(k2 as decimalv3(9,0)) from test_float_to_decimal32_1;
        """
        exception "Arithmetic overflow"
    }
    qt_castfloat_to_decimal32_3 """
        select cast(k2 as decimalv3(10,0)) from test_float_to_decimal32_1;
    """
    qt_castfloat_to_decimal32_4 """
        select cast(k2 as decimalv3(13,3)) from test_float_to_decimal32_1;
    """

    // negative
    prepare_test_float_to_decimal32_1()
    sql """
        insert into test_float_to_decimal32_1 values(1, -123456789.523);
    """
    test {
        sql """
            select cast(k2 as decimalv3(9,1)) from test_float_to_decimal32_1;
        """
        exception "overflow"
    }
    qt_cast_negative_float_to_decimal32_1 """
        select cast(k2 as decimalv3(9,0)) from test_float_to_decimal32_1;
    """
    qt_cast_negative_float_to_decimal32_2 """
        select cast(k2 as decimalv3(10,1)) from test_float_to_decimal32_1;
    """
    qt_cast_negative_float_to_decimal32_3 """
        select cast(k2 as decimalv3(12,3)) from test_float_to_decimal32_1;
    """
    qt_cast_negative_float_to_decimal32_4 """
        select cast(k2 as decimalv3(18,6)) from test_float_to_decimal32_1;
    """
    test {
        sql """
            select cast(k2 as decimalv3(38,30)) from test_float_to_decimal32_1;
        """
        exception "overflow"
    }
    qt_cast_negative_float_to_decimal32_5 """
        select cast(k2 as decimalv3(38,29)) from test_float_to_decimal32_1;
    """
    qt_cast_negative_float_to_decimal32_6 """
        select cast(k2 as decimalv3(38,26)) from test_float_to_decimal32_1;
    """
    sql "set enable_decimal256=true;"
    test {
        sql """
            select cast(k2 as decimalv3(76,68)) from test_float_to_decimal32_1;
        """
        exception "overflow"
    }
    /*
    qt_cast_negative_float_to_decimal32_7 """
        select cast(k2 as decimalv3(76,67)) from test_float_to_decimal32_1;
    """
    qt_cast_negative_float_to_decimal32_8 """
        select cast(k2 as decimalv3(76,64)) from test_float_to_decimal32_1;
    """
    */
    sql "set enable_decimal256=false;"
}