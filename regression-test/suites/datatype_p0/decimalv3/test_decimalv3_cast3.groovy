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

suite("test_decimalv3_cast3") {
    // test cast decimals to integer

    // cast decimal32 to integer
    def prepare_test_decimal32_to_integral_1 = {
        sql "drop table if exists test_decimal32_to_integral_1;"
        sql """
            CREATE TABLE test_decimal32_to_integral_1(
              k1 decimalv3(9, 3)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    // tiny int: [-128, 127]
    prepare_test_decimal32_to_integral_1()
    sql """
        insert into test_decimal32_to_integral_1 values (127.789), (99.999), (-128.789), (-99.999); 
    """
    qt_decimal32_to_int_1 """
        select cast(k1 as tinyint) from test_decimal32_to_integral_1 order by 1;
    """

    // small int: [-32768, 32767]
    prepare_test_decimal32_to_integral_1()
    sql """
        insert into test_decimal32_to_integral_1 values 
            (127.789), (99.999), (-128.789), (-99.999),
            (32767.789), (9999.999), (-32768.789), (-9999.999); 
    """
    qt_decimal32_to_int_2 """
        select cast(k1 as smallint) from test_decimal32_to_integral_1 order by 1;
    """
    // overflow
    prepare_test_decimal32_to_integral_1()
    sql """
        insert into test_decimal32_to_integral_1 values (32768.123);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal32_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal32_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    prepare_test_decimal32_to_integral_1()
    sql """
        insert into test_decimal32_to_integral_1 values (-32769.789);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal32_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal32_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }

    sql "drop table test_decimal32_to_integral_1;"

    // int: [-2147483648, 2147483647]
    def prepare_test_decimal32_to_integral_2 = {
        sql "drop table if exists test_decimal32_to_integral_2;"
        sql """
            CREATE TABLE test_decimal32_to_integral_2(
              k1 decimalv3(9, 0)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal32_to_integral_2()
    sql """
        insert into test_decimal32_to_integral_2 values 
            (127.0), (-128.0),
            (32767.0), (-32768.0),
            (999999999.0),(-999999999.0);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal32_to_integral_2 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal32_to_integral_2 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal32_to_int_3 """
        select cast(k1 as int) from test_decimal32_to_integral_2 order by 1;
    """
    qt_decimal32_to_int_4 """
        select cast(k1 as bigint) from test_decimal32_to_integral_2 order by 1;
    """
    qt_decimal32_to_int_5 """
        select cast(k1 as largeint) from test_decimal32_to_integral_2 order by 1;
    """

    // cast decimal64 to integer
    // [-9223372036854775808, 9223372036854775807]
    def prepare_test_decimal64_to_integral_1 = {
        sql "drop table if exists test_decimal64_to_integral_1;"
        sql """
            CREATE TABLE test_decimal64_to_integral_1(
              k1 decimalv3(18, 3)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal64_to_integral_1()
    sql """
        insert into test_decimal64_to_integral_1 values (127.789), (99.999), (-128.789), (-99.999); 
    """
    qt_decimal64_to_int_1 """
        select cast(k1 as tinyint) from test_decimal64_to_integral_1 order by 1;
    """
    prepare_test_decimal64_to_integral_1()
    sql """
        insert into test_decimal64_to_integral_1 values 
            (127.789), (99.999), (-128.789), (-99.999),
            (32767.789), (9999.999), (-32768.789), (-9999.999); 
    """
    qt_decimal64_to_int_2 """
        select cast(k1 as smallint) from test_decimal64_to_integral_1 order by 1;
    """
    // overflow
    prepare_test_decimal64_to_integral_1()
    sql """
        insert into test_decimal64_to_integral_1 values (32768.123);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal64_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal64_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    prepare_test_decimal64_to_integral_1()
    sql """
        insert into test_decimal64_to_integral_1 values (-32769.789);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal64_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal64_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal64_to_int_3 """
        select cast(k1 as int) from test_decimal64_to_integral_1 order by 1;
    """
    qt_decimal64_to_int_4 """
        select cast(k1 as bigint) from test_decimal64_to_integral_1 order by 1;
    """
    qt_decimal64_to_int_5 """
        select cast(k1 as largeint) from test_decimal64_to_integral_1 order by 1;
    """

    prepare_test_decimal64_to_integral_1()
    sql """
        insert into test_decimal64_to_integral_1 values 
            (999999999999999.999), (-999999999999999.999); 
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal64_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal64_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as int) from test_decimal64_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal64_to_int_6 """
        select cast(k1 as bigint) from test_decimal64_to_integral_1 order by 1;
    """
    qt_decimal64_to_int_7 """
        select cast(k1 as largeint) from test_decimal64_to_integral_1 order by 1;
    """
    sql "drop table test_decimal64_to_integral_1; "

    // cast decimal128 to integer
    // [-170141183460469231731687303715884105728, 170141183460469231731687303715884105727]
    def prepare_test_decimal128_to_integral_1 = {
        sql "drop table if exists test_decimal128_to_integral_1;"
        sql """
            CREATE TABLE test_decimal128_to_integral_1(
              k1 decimalv3(38, 3)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal128_to_integral_1()
    sql """
        insert into test_decimal128_to_integral_1 values (127.789), (99.999), (-128.789), (-99.999); 
    """
    qt_decimal128_to_int_1 """
        select cast(k1 as tinyint) from test_decimal128_to_integral_1 order by 1;
    """
    prepare_test_decimal128_to_integral_1()
    sql """
        insert into test_decimal128_to_integral_1 values 
            (127.789), (99.999), (-128.789), (-99.999),
            (32767.789), (9999.999), (-32768.789), (-9999.999); 
    """
    qt_decimal128_to_int_2 """
        select cast(k1 as smallint) from test_decimal128_to_integral_1 order by 1;
    """

    prepare_test_decimal128_to_integral_1()
    sql """
        insert into test_decimal128_to_integral_1 values (32768.123);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    prepare_test_decimal128_to_integral_1()
    sql """
        insert into test_decimal128_to_integral_1 values (-32769.789);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal128_to_int_3 """
        select cast(k1 as int) from test_decimal128_to_integral_1 order by 1;
    """
    qt_decimal128_to_int_4 """
        select cast(k1 as bigint) from test_decimal128_to_integral_1 order by 1;
    """
    qt_decimal128_to_int_5 """
        select cast(k1 as largeint) from test_decimal128_to_integral_1 order by 1;
    """

    prepare_test_decimal128_to_integral_1()
    sql """
        insert into test_decimal128_to_integral_1 values
          (99999999999999999999999999999999999.999), (-99999999999999999999999999999999999.999);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as int) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as bigint) from test_decimal128_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal128_to_int_6 """
        select cast(k1 as largeint) from test_decimal128_to_integral_1 order by 1;
    """
    sql "drop table test_decimal128_to_integral_1; "

    // cast decimal256 to integer
    // [-170141183460469231731687303715884105728, 170141183460469231731687303715884105727]
    sql "set enable_decimal256=true;"
    def prepare_test_decimal256_to_integral_1 = {
        sql "drop table if exists test_decimal256_to_integral_1;"
        sql """
            CREATE TABLE test_decimal256_to_integral_1(
              k1 decimalv3(76, 3)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values (127.789), (99.999), (-128.789), (-99.999); 
    """
    qt_decimal256_to_int_1 """
        select cast(k1 as tinyint) from test_decimal256_to_integral_1 order by 1;
    """
    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values 
            (127.789), (99.999), (-128.789), (-99.999),
            (32767.789), (9999.999), (-32768.789), (-9999.999); 
    """
    qt_decimal256_to_int_2 """
        select cast(k1 as smallint) from test_decimal256_to_integral_1 order by 1;
    """

    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values (32768.123);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values (-32769.789);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal256_to_int_3 """
        select cast(k1 as int) from test_decimal256_to_integral_1 order by 1;
    """
    qt_decimal256_to_int_4 """
        select cast(k1 as bigint) from test_decimal256_to_integral_1 order by 1;
    """
    qt_decimal256_to_int_5 """
        select cast(k1 as largeint) from test_decimal256_to_integral_1 order by 1;
    """


    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values
          (99999999999999999999999999999999999.999), (-99999999999999999999999999999999999.999);
    """
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as int) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as bigint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal256_to_int_6 """
        select cast(k1 as largeint) from test_decimal256_to_integral_1 order by 1;
    """

    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values
          (-170141183460469231731687303715884105728.123), (170141183460469231731687303715884105727.123);
    """
          //(999999999999999999999999999999999999.999), (-999999999999999999999999999999999999.999);
    test {
        sql """
        select cast(k1 as tinyint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as smallint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as int) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
        select cast(k1 as bigint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    qt_decimal256_to_int_6 """
        select cast(k1 as largeint) from test_decimal256_to_integral_1 order by 1;
    """

    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values
          (170141183460469231731687303715884105728.123);
    """
    test {
        sql """
        select cast(k1 as largeint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }

    prepare_test_decimal256_to_integral_1()
    sql """
        insert into test_decimal256_to_integral_1 values
          (-170141183460469231731687303715884105729.123);
    """
    test {
        sql """
        select cast(k1 as largeint) from test_decimal256_to_integral_1 order by 1;
        """
        exception "Arithmetic overflow"
    }
    sql "set enable_decimal256=false;"
}