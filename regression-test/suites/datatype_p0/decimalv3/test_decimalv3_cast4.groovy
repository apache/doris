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

suite("test_decimalv3_cast4") {
    // test cast decimals to float numbers
    // cast decimal128 to integer
    // [-170141183460469231731687303715884105728, 170141183460469231731687303715884105727]
    def prepare_test_decimal128_to_float_doubel_1 = {
        sql "drop table if exists prepare_test_decimal128_to_float_double_1;"
        sql """
            CREATE TABLE prepare_test_decimal128_to_float_double_1(
              k1 decimalv3(38, 3)
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }
    prepare_test_decimal128_to_float_doubel_1()
    sql """
        insert into prepare_test_decimal128_to_float_double_1 values
            (127.789), (99.999), (-128.789), (-99.999),
            (32767.789), (9999.999), (-32768.789), (-9999.999),
            (32768.123), (-32769.789),
            (99999999999999999999999999999999999.999),
            (-99999999999999999999999999999999999.999);
    """
    qt_decimal128_to_float_1 """
        select cast(k1 as float) from prepare_test_decimal128_to_float_double_1 order by 1;
    """
    qt_decimal128_to_double_1 """
        select cast(k1 as double) from prepare_test_decimal128_to_float_double_1 order by 1;
    """
}
