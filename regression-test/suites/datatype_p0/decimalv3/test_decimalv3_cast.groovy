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

suite("test_decimalv3_cast") {
    qt_cast0 """ select cast('0.00164999999999998' as decimalv3(9,0)); """
    qt_cast1 """ select cast('0.00164999999999998' as decimalv3(10,0)); """
    qt_cast2 """ select cast('0.000000000001234567890' as decimalv3(18,0)); """
    qt_cast3 """ select cast('0.000000000001234567890' as decimalv3(19,0)); """
    qt_cast4 """ select cast('0.00000000000000000000000000000012345678901' as decimalv3(38,0)); """

    // test cast between decimal types
    // 5.4
    // cast decimal32
    def prepare_test_decimal32_cast1 = {
        sql "drop table if exists test_decimal32_cast1;"
        sql """
            CREATE TABLE test_decimal32_cast1(
              k1 decimalv3(9, 4)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal32_cast1()
    sql """
        insert into test_decimal32_cast1 values (99999.9999);
    """
    qt_cast32_select_all """
        select * from test_decimal32_cast1 order by 1;
    """

    // old type: 5.4
    //////////////////////////////////////////
    // cast to wider scale: x.5
    //////////////////////////////////////////

    //======= wider integral: 6.5
    qt_cast32_wider_scale_1 """
        select cast(k1 as decimalv3(11, 5)) from test_decimal32_cast1;
    """
    qt_cast32_wider_scale_2 """
        select cast(k1 as decimalv3(38, 5)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_wider_scale_3 """
        select cast(k1 as decimalv3(76, 5)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=false;"

    // old type: 5.4
    //======= same integral: 5.5
    qt_cast32_wider_scale_4 """
        select cast(k1 as decimalv3(10, 5)) from test_decimal32_cast1;
    """

    //======= narrow integral: 4.5
    test {
        // test multiply result overflow
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal32_cast1;
        """
        exception "Arithmetic overflow"
    }
    // test multiply result not overflow
    prepare_test_decimal32_cast1()
    sql """
        insert into test_decimal32_cast1 values (9999.9999);
    """
    qt_cast32_select_all2 """
        select * from test_decimal32_cast1 order by 1;
    """
    qt_cast32_wider_scale_5 """
        select cast(k1 as decimalv3(9, 5)) from test_decimal32_cast1;
    """

    // old type: 5.4
    // test multiply result not overflow, but cast result overflow
    prepare_test_decimal32_cast1()
    sql """
        insert into test_decimal32_cast1 values (10000.0001);
    """
    qt_cast32_select_all3 """
        select * from test_decimal32_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal32_cast1;
        """
        exception "Arithmetic overflow"
    }

    prepare_test_decimal32_cast1();
    sql """
        insert into test_decimal32_cast1 values (99999.9999);
    """

    // old type: 5.4
    //////////////////////////////////////////
    // cast to same scale: x.4
    //////////////////////////////////////////

    //======= wider integral: 6.4
    qt_cast32_same_scale_1 """
        select cast(k1 as decimalv3(10, 4)) from test_decimal32_cast1;
    """
    qt_cast32_same_scale_2 """
        select cast(k1 as decimalv3(18, 4)) from test_decimal32_cast1;
    """
    qt_cast32_same_scale_3 """
        select cast(k1 as decimalv3(38, 4)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_same_scale_4 """
        select cast(k1 as decimalv3(76, 4)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=false;"

    //======= same integral: 5.4
    qt_cast32_same_scale_5 """
        select cast(k1 as decimalv3(9, 4)) from test_decimal32_cast1;
    """

    // old type: 5.4
    // cast to same scale: x.4
    //======= narrow integral: 4.4
    test {
        sql """
            select cast(k1 as decimalv3(8, 4)) from test_decimal32_cast1;
        """
        exception "Arithmetic overflow"
    }

    // not overflow
    prepare_test_decimal32_cast1()
    sql """
        insert into test_decimal32_cast1 values (9999.9999);
    """
    qt_cast32_same_scale_6 """
        select cast(k1 as decimalv3(8, 4)) from test_decimal32_cast1;
    """

    // old type: 5.4
    //////////////////////////////////////////
    // cast to narrow scale: x.3
    //////////////////////////////////////////

    //======= wider integral: 6.3
    qt_cast32_narrow_scale_1 """
        select cast(k1 as decimalv3(9, 3)) from test_decimal32_cast1;
    """
    qt_cast32_narrow_scale_2 """
        select cast(k1 as decimalv3(18, 3)) from test_decimal32_cast1;
    """
    qt_cast32_narrow_scale_3 """
        select cast(k1 as decimalv3(38, 3)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_narrow_scale_4 """
        select cast(k1 as decimalv3(76, 3)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=false;"

    // old type: 5.4
    // cast to narrow scale: x.3
    //======= same integral: 5.3
    qt_cast32_narrow_scale_5 """
        select cast(k1 as decimalv3(8, 3)) from test_decimal32_cast1;
    """

    // old type: 5.4
    // cast to narrow scale: x.3
    //======= narrow integral: 4.3
    // integral overflow
    test {
        sql """
            select cast(k1 as decimalv3(7, 3)) from test_decimal32_cast1;
        """
        exception "Arithmetic overflow"
    }

    // integral overflow: overflow after round
    prepare_test_decimal32_cast1();
    sql """
        insert into test_decimal32_cast1 values (9999.9999);
    """
    test {
        sql """
            select cast(k1 as decimalv3(7, 3)) from test_decimal32_cast1;
        """
        exception "Arithmetic overflow"
    }

    // integral not overflow
    prepare_test_decimal32_cast1();
    sql """
        insert into test_decimal32_cast1 values (9999.9989);
    """
    qt_cast32_narrow_scale_6 """
        select cast(k1 as decimalv3(7, 3)) from test_decimal32_cast1;
    """
    sql "drop table test_decimal32_cast1; "


    // cast negative decimal32
    prepare_test_decimal32_cast1()
    sql """
        insert into test_decimal32_cast1 values (-99999.9999);
    """
    // old type: 5.4
    //////////////////////////////////////////
    // cast to wider scale: x.5
    //////////////////////////////////////////
    //======= wider integral: 6.5
    qt_cast32_negative_wider_scale_1 """
        select cast(k1 as decimalv3(11, 5)) from test_decimal32_cast1;
    """
    qt_cast32_negative_wider_scale_2 """
        select cast(k1 as decimalv3(38, 5)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_negative_wider_scale_3 """
        select cast(k1 as decimalv3(76, 5)) from test_decimal32_cast1;
    """
    sql "set enable_decimal256=false;"

    //======= same integral: 5.5
    qt_cast32_negative_wider_scale_4 """
        select cast(k1 as decimalv3(10, 5)) from test_decimal32_cast1;
    """

    //======= narrow integral: 4.5
    test {
        // test multiply result overflow
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal32_cast1;
        """
        exception "Arithmetic overflow"
    }
    // test multiply result not overflow
    prepare_test_decimal32_cast1()
    sql """
        insert into test_decimal32_cast1 values (-9999.9999);
    """
    qt_cast32_negative_wider_scale_5 """
        select cast(k1 as decimalv3(9, 5)) from test_decimal32_cast1;
    """

    // old type: 5.4
    // cast to wider scale: x.5
    // narrow integral: 4.5
    // test multiply result not overflow, but cast result overflow
    prepare_test_decimal32_cast1()
    sql """
        insert into test_decimal32_cast1 values (-10000.0001);
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal32_cast1;
        """
        exception "Arithmetic overflow"
    }

    def prepare_test_decimal32_cast2 = {
        // type: 2.4
        sql "drop table if exists test_decimal32_cast2;"
        sql """
            CREATE TABLE test_decimal32_cast2(
              k1 decimalv3(6, 4)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal32_cast2()
    sql """
        insert into test_decimal32_cast2 values (99.9999);
    """

    // old type: 2.4
    //////////////////////////////////////////
    // cast to wider scale: x.5
    //////////////////////////////////////////

    //======= wider integral: 3.5
    qt_cast32_2_wider_scale_1 """
        select cast(k1 as decimalv3(8, 5)) from test_decimal32_cast2;
    """
    qt_cast32_2_wider_scale_2 """
        select cast(k1 as decimalv3(18, 5)) from test_decimal32_cast2;
    """
    qt_cast32_2_wider_scale_2 """
        select cast(k1 as decimalv3(38, 5)) from test_decimal32_cast2;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_2_wider_scale_3 """
        select cast(k1 as decimalv3(76, 5)) from test_decimal32_cast2;
    """
    sql "set enable_decimal256=false;"

    // old type: 2.4
    //======= same integral: 2.5
    qt_cast32_wider_scale_4 """
        select cast(k1 as decimalv3(7, 5)) from test_decimal32_cast2;
    """

    //======= narrow integral: 1.5
    // integral part overflow
    test {
        sql """
            select cast(k1 as decimalv3(6, 5)) from test_decimal32_cast2;
        """
        exception "Arithmetic overflow"
    }

    // integral part not overflow
    prepare_test_decimal32_cast2()
    sql """
        insert into test_decimal32_cast2 values (9.9999);
    """
    qt_cast32_wider_scale_5 """
        select cast(k1 as decimalv3(6, 5)) from test_decimal32_cast2;
    """

    // old type: 2.4
    //////////////////////////////////////////
    // cast to same scale: x.4
    //////////////////////////////////////////
    prepare_test_decimal32_cast2()
    sql """
        insert into test_decimal32_cast2 values (99.9999);
    """

    //======= wider integral: 3.4
    qt_cast32_2_same_scale_1 """
        select cast(k1 as decimalv3(7, 4)) from test_decimal32_cast2;
    """
    qt_cast32_2_same_scale_2 """
        select cast(k1 as decimalv3(18, 4)) from test_decimal32_cast2;
    """
    qt_cast32_2_same_scale_3 """
        select cast(k1 as decimalv3(38, 4)) from test_decimal32_cast2;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_2_same_scale_4 """
        select cast(k1 as decimalv3(76, 4)) from test_decimal32_cast2;
    """
    sql "set enable_decimal256=false;"

    // old type: 2.4
    // cast to same scale: x.4
    //======= same integral: 2.4
    qt_cast32_2_same_scale_5 """
        select cast(k1 as decimalv3(6, 4)) from test_decimal32_cast2;
    """

    // cast to same scale: x.4
    //======= narrow integral: 1.4
    // integral part overflow
    test {
        sql """
            select cast(k1 as decimalv3(5, 4)) from test_decimal32_cast2;
        """
        exception "Arithmetic overflow"
    }

    // integral part not overflow
    prepare_test_decimal32_cast2()
    sql """
        insert into test_decimal32_cast2 values (9.9999);
    """
    qt_cast32_2_same_scale_6 """
        select cast(k1 as decimalv3(5, 4)) from test_decimal32_cast2;
    """

    // old type: 2.4
    //////////////////////////////////////////
    // cast to narrow scale: x.3
    //////////////////////////////////////////
    prepare_test_decimal32_cast2()
    sql """
        insert into test_decimal32_cast2 values (99.9999);
    """

    // wider integral: 3.3
    qt_cast32_2_narrow_scale_1 """
        select cast(k1 as decimalv3(6, 3)) from test_decimal32_cast2;
    """
    qt_cast32_2_narrow_scale_2 """
        select cast(k1 as decimalv3(18, 3)) from test_decimal32_cast2;
    """
    qt_cast32_2_narrow_scale_3 """
        select cast(k1 as decimalv3(38, 3)) from test_decimal32_cast2;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_2_narrow_scale_4 """
        select cast(k1 as decimalv3(76, 3)) from test_decimal32_cast2;
    """
    sql "set enable_decimal256=false;"

    // old type: 2.4
    // cast to narrow scale: x.3
    // same integral: 2.3
    // overflow after round
    test {
        sql """
        select cast(k1 as decimalv3(5, 3)) from test_decimal32_cast2;
        """
        exception "Arithmetic overflow"
    }
    // not overflow after round
    prepare_test_decimal32_cast2()
    sql """
        insert into test_decimal32_cast2 values (99.9989);
    """
    qt_cast32_2_narrow_scale_5 """
        select cast(k1 as decimalv3(5, 3)) from test_decimal32_cast2;
    """

    // old type: 2.4
    // cast to narrow scale: x.3
    // narrow integral: 1.3
    // integral overflow
    test {
        sql """
            select cast(k1 as decimalv3(4, 3)) from test_decimal32_cast2;
        """
        exception "Arithmetic overflow"
    }

    // integral overflow: overflow after round
    prepare_test_decimal32_cast2();
    sql """
        insert into test_decimal32_cast2 values (9.9999);
    """
    test {
        sql """
            select cast(k1 as decimalv3(4, 3)) from test_decimal32_cast2;
        """
        exception "Arithmetic overflow"
    }

    // integral not overflow
    prepare_test_decimal32_cast2();
    sql """
        insert into test_decimal32_cast2 values (9.9989);
    """
    qt_cast32_2_narrow_scale_6 """
        select cast(k1 as decimalv3(4, 3)) from test_decimal32_cast2;
    """
    sql "drop table test_decimal32_cast2;"

    def prepare_test_decimal32_cast3 = {
        // type: 9.0
        sql "drop table if exists test_decimal32_cast3;"
        sql """
            CREATE TABLE test_decimal32_cast3(
              k1 decimalv3(9, 0)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal32_cast3()
    sql """
        insert into test_decimal32_cast3 values(999999999.0);
    """
    test {
        // multiply not overflow, but result integral part overflow
        sql """
            select cast(k1 as decimalv3(17, 9)) from test_decimal32_cast3;
        """
        exception "Arithmetic overflow"
    }
    test {
        // multiply overflow: 999999999 * 10^10, result digit count: 19
        sql """
            select cast(k1 as decimalv3(18, 10)) from test_decimal32_cast3;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 30)) from test_decimal32_cast3;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=true;"
    test {
        sql """
            select cast(k1 as decimalv3(76, 68)) from test_decimal32_cast3;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"
    qt_cast32_3_to_much_bigger_scale_1 """
        select cast(k1 as decimalv3(18, 9)) from test_decimal32_cast3;
    """
    qt_cast32_3_to_much_bigger_scale_2 """
        select cast(k1 as decimalv3(19, 10)) from test_decimal32_cast3;
    """
    qt_cast32_3_to_much_bigger_scale_3 """
        select cast(k1 as decimalv3(38, 29)) from test_decimal32_cast3;
    """
    sql "set enable_decimal256=true;"
    qt_cast32_3_to_much_bigger_scale_4 """
        select cast(k1 as decimalv3(76, 67)) from test_decimal32_cast3;
    """
    sql "set enable_decimal256=false;"

    // cast decimal64
    def prepare_test_decimal64_cast1 = {
        // 12.6
        sql "drop table if exists test_decimal64_cast1;"
        sql """
            CREATE TABLE test_decimal64_cast1(
              k1 decimalv3(18, 6)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (999999999999.999999);
    """
    qt_cast64_select_all """
        select * from test_decimal64_cast1 order by 1;
    """

    // old type: 12.6
    //////////////////////////////////////////
    // cast to wider scale: x.7
    //////////////////////////////////////////

    //======= wider integral: 13.7
    qt_cast64_wider_scale_1 """
        select cast(k1 as decimalv3(20, 7)) from test_decimal64_cast1;
    """
    qt_cast64_wider_scale_2 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal64_cast1;
    """
    sql "set enable_decimal256=true;"
    qt_cast64_wider_scale_3 """
        select cast(k1 as decimalv3(76, 7)) from test_decimal64_cast1;
    """
    sql "set enable_decimal256=false;"

    // cast to wider scale: x.7
    //======= same integral: 12.7
    qt_cast64_wider_scale_4 """
        select cast(k1 as decimalv3(19, 7)) from test_decimal64_cast1;
    """

    // old type: 12.6
    // cast to wider scale: x.7
    //======= narrow integral: 11.7
    // integral part overflow
    test {
        sql """
            select cast(k1 as decimalv3(18, 7)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(8, 7)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    
    // cast to wider scale: x.7
    //======= narrow integral: 11.7
    // integral part not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (99999999999.999999);
    """
    qt_cast64_wider_scale_5 """
        select cast(k1 as decimalv3(18, 7)) from test_decimal64_cast1;
    """
    // cast to decimal32 overflow
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(8, 7)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // cast to decimal32 not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (99.999999);
    """
    qt_cast64_wider_scale_6 """
        select cast(k1 as decimalv3(9, 7)) from test_decimal64_cast1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(8, 7)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // cast to decimal32 not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (9.999999);
    """
    qt_cast64_wider_scale_7 """
        select cast(k1 as decimalv3(9, 7)) from test_decimal64_cast1;
    """
    qt_cast64_wider_scale_8 """
        select cast(k1 as decimalv3(8, 7)) from test_decimal64_cast1;
    """

    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (999999999999.999999);
    """
    // old type: 12.6
    //////////////////////////////////////////
    // cast to same scale: x.6
    //////////////////////////////////////////

    //======= wider integral: 13.6
    qt_cast64_same_scale_1 """
        select cast(k1 as decimalv3(19, 6)) from test_decimal64_cast1;
    """
    qt_cast64_same_scale_2 """
        select cast(k1 as decimalv3(38, 6)) from test_decimal64_cast1;
    """
    sql "set enable_decimal256=true;"
    qt_cast64_same_scale_3 """
        select cast(k1 as decimalv3(76, 6)) from test_decimal64_cast1;
    """
    sql "set enable_decimal256=false;"

    // cast to same scale: x.6
    //======= same integral: 12.6
    qt_cast64_same_scale_4 """
        select cast(k1 as decimalv3(18, 6)) from test_decimal64_cast1;
    """

    // old type: 12.6
    // cast to same scale: x.6
    //======= narrow integral: 11.6
    // integral part overflow
    test {
        sql """
            select cast(k1 as decimalv3(17, 6)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal32 overflow
    test {
        sql """
            select cast(k1 as decimalv3(9, 6)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(8, 6)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // cast to same scale: x.6
    //======= narrow integral: 11.6
    // integral part not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (99999999999.999999);
    """
    qt_cast64_same_scale_5 """
        select cast(k1 as decimalv3(17, 6)) from test_decimal64_cast1;
    """
    // to decimal32 overflow
    test {
        sql """
            select cast(k1 as decimalv3(9, 6)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(8, 6)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // cast to same scale: x.6
    //======= narrow integral: 3.6
    // to decimal32 not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (999.999999);
    """
    qt_cast64_same_scale_6 """
        select cast(k1 as decimalv3(9, 6)) from test_decimal64_cast1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(8, 6)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal32 not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (99.999999);
    """
    qt_cast64_same_scale_7 """
        select cast(k1 as decimalv3(9, 6)) from test_decimal64_cast1;
    """
    qt_cast64_same_scale_7 """
        select cast(k1 as decimalv3(8, 6)) from test_decimal64_cast1;
    """

    // old type: 12.6
    //////////////////////////////////////////
    // cast to narrow scale: x.5
    //////////////////////////////////////////
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (999999999999.999999);
    """

    //======= wider integral: 13.5
    qt_cast64_narrow_scale_1 """
        select cast(k1 as decimalv3(18, 5)) from test_decimal64_cast1;
    """
    qt_cast64_narrow_scale_2 """
        select cast(k1 as decimalv3(38, 5)) from test_decimal64_cast1;
    """
    sql "set enable_decimal256=true;"
    qt_cast64_narrow_scale_3 """
        select cast(k1 as decimalv3(76, 5)) from test_decimal64_cast1;
    """
    sql "set enable_decimal256=false;"

    // old type: 12.6
    // cast to narrow scale: x.5
    //======= same integral: 12.5
    // overflow after round
    test {
        sql """
        select cast(k1 as decimalv3(17, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    // not overflow after round
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (999999999999.999994);
    """
    qt_cast64_narrow_scale_4 """
        select cast(k1 as decimalv3(17, 5)) from test_decimal64_cast1;
    """

    // old type: 12.6
    // cast to narrow scale: x.5
    //======= narrow integral: 11.5
    // integral overflow
    test {
        sql """
            select cast(k1 as decimalv3(16, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // integral overflow after round
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (99999999999.999999);
    """
    test {
        sql """
            select cast(k1 as decimalv3(16, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // integral not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (99999999999.999989);
    """
    qt_cast64_narrow_scale_5 """
        select cast(k1 as decimalv3(16, 5)) from test_decimal64_cast1;
    """

    // to decimal32 overflow
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(8, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal32 overflow after round
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (9999.999999);
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(8, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal32 not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (9999.999989);
    """
    qt_cast64_narrow_scale_6 """
        select cast(k1 as decimalv3(9, 5)) from test_decimal64_cast1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(8, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal32 overflow after round
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (999.999999);
    """
    qt_cast64_narrow_scale_7 """
        select cast(k1 as decimalv3(9, 5)) from test_decimal64_cast1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(8, 5)) from test_decimal64_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal32 not overflow
    prepare_test_decimal64_cast1()
    sql """
        insert into test_decimal64_cast1 values (999.999989);
    """
    qt_cast64_narrow_scale_8 """
        select cast(k1 as decimalv3(9, 5)) from test_decimal64_cast1;
    """
    qt_cast64_narrow_scale_9 """
        select cast(k1 as decimalv3(8, 5)) from test_decimal64_cast1;
    """
    sql "drop table test_decimal64_cast1;"


    // cast decimal128
    def prepare_test_decimal128_cast1 = {
        // 32.6
        sql "drop table if exists test_decimal128_cast1;"
        sql """
            CREATE TABLE test_decimal128_cast1(
              k1 decimalv3(38, 6)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (99999999999999999999999999999999.999999),
            (-99999999999999999999999999999999.999999);
    """

    // old type: 32.6
    //////////////////////////////////////////
    // cast to wider scale: x.7
    //////////////////////////////////////////

    //======= wider integral: 33.7
    sql "set enable_decimal256=true;"
    qt_cast128_wider_scale_1 """
        select cast(k1 as decimalv3(40, 7)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_wider_scale_2 """
        select cast(k1 as decimalv3(76, 7)) from test_decimal128_cast1 order by 1;
    """

    //======= same integral: 32.7
    qt_cast128_wider_scale_3 """
        select cast(k1 as decimalv3(39, 7)) from test_decimal128_cast1 order by 1;
    """
    sql "set enable_decimal256=false;"

    // narrow integral overflow
    //======= narrow integral: 31.7
    test {
        sql """
            select cast(k1 as decimalv3(38, 7)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 7)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to wider scale: x.7
    //======= narrow integral: 31.7
    // narrow integral 128 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (9999999999999999999999999999999.999999),
            (-9999999999999999999999999999999.999999);
    """
    qt_cast128_wider_scale_4 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal128_cast1 order by 1;
    """
    // to decimal64 and decimal32 still overflow
    test {
        sql """
            select cast(k1 as decimalv3(18, 7)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to wider scale: x.7
    //======= narrow integral
    // narrow integral 64 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (99999999999.999999),
            (-99999999999.999999);
    """
    qt_cast128_wider_scale_5 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_wider_scale_6 """
        select cast(k1 as decimalv3(18, 7)) from test_decimal128_cast1 order by 1;
    """
    // to decimal32 still overflow
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to wider scale: x.7
    //======= narrow integral
    // narrow integral 32 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (99.999999), (-99.999999);
    """
    qt_cast128_wider_scale_7 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_wider_scale_8 """
        select cast(k1 as decimalv3(18, 7)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_wider_scale_9 """
        select cast(k1 as decimalv3(9, 7)) from test_decimal128_cast1 order by 1;
    """

    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (99999999999999999999999999999999.999999),
            (-99999999999999999999999999999999.999999);
    """

    // old type: 32.6
    //////////////////////////////////////////
    // cast to same scale: x.6
    //////////////////////////////////////////

    //======= wider integral: 33.6
    sql "set enable_decimal256=true;"
    qt_cast128_same_scale_1 """
        select cast(k1 as decimalv3(39, 6)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_same_scale_2 """
        select cast(k1 as decimalv3(76, 6)) from test_decimal128_cast1 order by 1;
    """
    sql "set enable_decimal256=false;"

    // old type: 32.6
    // cast to same scale: x.6
    //======= same integral: 32.6
    qt_cast128_same_scale_3 """
        select cast(k1 as decimalv3(38, 6)) from test_decimal128_cast1 order by 1;
    """

    // old type: 32.6
    // cast to same scale: x.6
    //======= narrow integral: 31.6
    // overflow
    test {
        sql """
            select cast(k1 as decimalv3(37, 6)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    // to decimal64 and decimal32 overflow
    test {
        sql """
            select cast(k1 as decimalv3(18, 6)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 6)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to same scale: x.6
    //======= narrow integral: 31.6
    // to decimal128 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (9999999999999999999999999999999.999999),
            (-9999999999999999999999999999999.999999);
    """
    qt_cast128_same_scale_4 """
        select cast(k1 as decimalv3(37, 6)) from test_decimal128_cast1 order by 1;
    """
    // to decimal64 and decimal32 still overflow
    test {
        sql """
            select cast(k1 as decimalv3(18, 6)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 6)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to same scale: x.6
    //======= narrow integral
    // to decimal64 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (999999999999.999999),
            (-999999999999.999999);
    """
    qt_cast128_same_scale_5 """
        select cast(k1 as decimalv3(37, 6)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_same_scale_6 """
        select cast(k1 as decimalv3(18, 6)) from test_decimal128_cast1 order by 1;
    """
    // to decimal32 overflow
    test {
        sql """
            select cast(k1 as decimalv3(9, 6)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to same scale: x.6
    //======= narrow integral
    // to decimal32 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values (999.999999), (-999.999999);
    """
    qt_cast128_same_scale_7 """
        select cast(k1 as decimalv3(37, 6)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_same_scale_8 """
        select cast(k1 as decimalv3(18, 6)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_same_scale_9 """
        select cast(k1 as decimalv3(9, 6)) from test_decimal128_cast1 order by 1;
    """

    // old type: 32.6
    //////////////////////////////////////////
    // cast to narrow scale: x.5
    //////////////////////////////////////////

    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (99999999999999999999999999999999.999999),
            (-99999999999999999999999999999999.999999);
    """

    //======= wider integral: 33.5
    qt_cast128_narrow_scale_1 """
        select cast(k1 as decimalv3(38, 5)) from test_decimal128_cast1 order by 1;
    """
    sql "set enable_decimal256=true;"
    qt_cast128_narrow_scale_2 """
        select cast(k1 as decimalv3(76, 5)) from test_decimal128_cast1 order by 1;
    """
    sql "set enable_decimal256=false;"

    //======= same integral: 32.5
    // overflow after round
    test {
        sql """
        select cast(k1 as decimalv3(37, 5)) from test_decimal128_cast1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (99999999999999999999999999999999.999989),
            (-99999999999999999999999999999999.999989);
    """
    qt_cast128_narrow_scale_3 """
        select cast(k1 as decimalv3(37, 5)) from test_decimal128_cast1 order by 1;
    """

    // old type: 32.6
    // cast to narrow scale: x.5
    //======= narrow integral: 31.5
    test {
        sql """
            select cast(k1 as decimalv3(36, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to narrow scale: x.5
    //======= narrow integral: 31.5
    // to decimal128 overflow after round
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (9999999999999999999999999999999.999999),
            (-9999999999999999999999999999999.999999);
    """
    test {
        sql """
            select cast(k1 as decimalv3(36, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to narrow scale: x.5
    //======= narrow integral: 31.5
    // to decimal128 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (9999999999999999999999999999999.999989),
            (-9999999999999999999999999999999.999989);
    """
    qt_cast128_narrow_scale_4 """
        select cast(k1 as decimalv3(36, 5)) from test_decimal128_cast1 order by 1;
    """
    // to decimal64 and decimal32 still overflow
    test {
        sql """
            select cast(k1 as decimalv3(18, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to narrow scale: x.5
    //======= narrow integral
    // to decimal64 overflow after round
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (9999999999999.999999),
            (-9999999999999.999999);
    """
    qt_cast128_narrow_scale_5 """
        select cast(k1 as decimalv3(36, 5)) from test_decimal128_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(18, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to narrow scale: x.5
    //======= narrow integral
    // to decimal64 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values
            (9999999999999.999989),
            (-9999999999999.999989);
    """
    qt_cast128_narrow_scale_6 """
        select cast(k1 as decimalv3(36, 5)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_narrow_scale_7 """
        select cast(k1 as decimalv3(18, 5)) from test_decimal128_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to narrow scale: x.5
    //======= narrow integral
    // to decimal32 overflow after round
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values (9999.999999), (-9999.999999);
    """
    qt_cast128_narrow_scale_8 """
        select cast(k1 as decimalv3(36, 5)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_narrow_scale_9 """
        select cast(k1 as decimalv3(18, 5)) from test_decimal128_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 5)) from test_decimal128_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 32.6
    // cast to narrow scale: x.5
    //======= narrow integral
    // to decimal32 not overflow
    prepare_test_decimal128_cast1()
    sql """
        insert into test_decimal128_cast1 values (9999.999989), (-9999.999989);
    """
    qt_cast128_narrow_scale_10 """
        select cast(k1 as decimalv3(36, 5)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_narrow_scale_11 """
        select cast(k1 as decimalv3(18, 5)) from test_decimal128_cast1 order by 1;
    """
    qt_cast128_narrow_scale_12 """
        select cast(k1 as decimalv3(9, 5)) from test_decimal128_cast1 order by 1;
    """
    sql "drop table test_decimal128_cast1;"

    // cast decimal256
    sql "set enable_decimal256=true;"
    def prepare_test_decimal256_cast1 = {
        // 62.8
        sql "drop table if exists test_decimal256_cast1;"
        sql """
            CREATE TABLE test_decimal256_cast1(
              k1 decimalv3(70, 8)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (99999999999999999999999999999999999999999999999999999999999999.99999999),
            (-99999999999999999999999999999999999999999999999999999999999999.99999999);
    """

    // old type: 62.8
    //////////////////////////////////////////
    // cast to wider scale: x.9
    //////////////////////////////////////////

    //======= wider integral: 63.9
    qt_cast256_wider_scale_1 """
        select cast(k1 as decimalv3(72, 9)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_2 """
        select cast(k1 as decimalv3(76, 9)) from test_decimal256_cast1 order by 1;
    """

    //======= same integral: 62.9
    qt_cast256_wider_scale_3 """
        select cast(k1 as decimalv3(71, 9)) from test_decimal256_cast1 order by 1;
    """

    //======= narrow integral: 61.9
    test {
        sql """
        select cast(k1 as decimalv3(70, 9)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(38, 9)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(18, 9)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as decimalv3(9, 9)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 62.8
    //////////////////////////////////////////
    // cast to same scale: x.8
    //////////////////////////////////////////

    //======= wider integral: 63.8
    qt_cast256_wider_scale_4 """
        select cast(k1 as decimalv3(71, 8)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_5 """
        select cast(k1 as decimalv3(76, 8)) from test_decimal256_cast1 order by 1;
    """

    //======= same integral: 62.8
    qt_cast256_wider_scale_6 """
        select cast(k1 as decimalv3(70, 8)) from test_decimal256_cast1 order by 1;
    """

    //======= narrow integral: 61.8
    test {
        sql """
            select cast(k1 as decimalv3(69, 8)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 8)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 8)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 8)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 62.8
    // cast to same scale: x.8
    //======= narrow integral: 61.8
    // to decimal256 not overflow
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (9999999999999999999999999999999999999999999999999999999999999.99999999),
            (-9999999999999999999999999999999999999999999999999999999999999.99999999);
    """
    qt_cast256_wider_scale_7 """
        select cast(k1 as decimalv3(69, 8)) from test_decimal256_cast1 order by 1;
    """

    // old type: 62.8
    //////////////////////////////////////////
    // cast to narrow scale: x.7
    //////////////////////////////////////////

    //======= wider integral: 63.7
    qt_cast256_wider_scale_8 """
        select cast(k1 as decimalv3(70, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_9 """
        select cast(k1 as decimalv3(76, 7)) from test_decimal256_cast1 order by 1;
    """

    //======= same integral: 62.7
    qt_cast256_wider_scale_10 """
        select cast(k1 as decimalv3(69, 7)) from test_decimal256_cast1 order by 1;
    """

    //======= narrow integral: 61.7
    test {
        sql """
            select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // old type: 62.8
    // cast to narrow scale: x.7
    //======= narrow integral: 61.7
    // to decimal256 overflow after round
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (9999999999999999999999999999999999999999999999999999999999999.99999999),
            (-9999999999999999999999999999999999999999999999999999999999999.99999999);
    """
    test {
        sql """
            select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal256 not overflow
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (9999999999999999999999999999999999999999999999999999999999999.99999989),
            (-9999999999999999999999999999999999999999999999999999999999999.99999989);
    """
    qt_cast256_wider_scale_11 """
        select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1 order by 1;
    """
    // to decimal128, decimal64, decimal32 overflow
    test {
        sql """
            select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(18, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal128 overflow after round
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (9999999999999999999999999999999.99999999),
            (-9999999999999999999999999999999.99999999);
    """
    qt_cast256_wider_scale_12 """
        select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal128 not overflow
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (9999999999999999999999999999999.99999989),
            (-9999999999999999999999999999999.99999989);
    """
    qt_cast256_wider_scale_13 """
        select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_14 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(18, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal64 overflow after round
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (99999999999.99999999),
            (-99999999999.99999999);
    """
    qt_cast256_wider_scale_15 """
        select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_16 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(18, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal64 not overflow
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (99999999999.99999989),
            (-99999999999.99999989);
    """
    qt_cast256_wider_scale_17 """
        select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_18 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_19 """
        select cast(k1 as decimalv3(18, 7)) from test_decimal256_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal32 overflow after round
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (99.99999999),
            (-99.99999999);
    """
    qt_cast256_wider_scale_20 """
        select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_21 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_22 """
        select cast(k1 as decimalv3(18, 7)) from test_decimal256_cast1 order by 1;
    """
    test {
        sql """
            select cast(k1 as decimalv3(9, 7)) from test_decimal256_cast1;
        """
        exception "Arithmetic overflow"
    }

    // to decimal32 not overflow
    prepare_test_decimal256_cast1()
    sql """
        insert into test_decimal256_cast1 values
            (99.99999989),
            (-99.99999989);
    """
    qt_cast256_wider_scale_23 """
        select cast(k1 as decimalv3(68, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_24 """
        select cast(k1 as decimalv3(38, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_25 """
        select cast(k1 as decimalv3(18, 7)) from test_decimal256_cast1 order by 1;
    """
    qt_cast256_wider_scale_26 """
        select cast(k1 as decimalv3(9, 7)) from test_decimal256_cast1 order by 1;
    """
    sql "drop table test_decimal256_cast1;"

}
