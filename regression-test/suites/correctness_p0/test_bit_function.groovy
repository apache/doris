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

suite("test_bit_functions") {
    qt_select 'select bitand(123456, 321.0), bitor(123456, 321.0), bitxor(123456, 321.0), bitnot(321.0);'

    qt_bit_count 'select bit_count(number) from numbers("number"="10");'
    qt_bit_count 'select bit_count(0);'
    qt_bit_count 'select bit_count(-1);'
    qt_bit_count 'select bit_count(cast (-1 as tinyint));'    // 8
    qt_bit_count 'select bit_count(cast (-1 as smallint));'   // 16
    qt_bit_count 'select bit_count(cast (-1 as int));'        // 32
    qt_bit_count 'select bit_count(cast (-1 as bigint));'     // 64
    qt_bit_count 'select bit_count(cast (-1 as largeint));'   // 128
    qt_bit_count_TINYINT_MAX 'select bit_count(cast (127 as tinyint));'                 // TINYINT_MAX
    qt_bit_count_TINYINT_MIN 'select bit_count(cast (-128 as tinyint));'                // TINYINT_MIN
    qt_bit_count_SMALLINT_MAX 'select bit_count(cast (32767 as smallint));'             // SMALLINT_MAX
    qt_bit_count_SMALLINT_MIN 'select bit_count(cast (-32768 as smallint));'            // SMALLINT_MIN
    qt_bit_count_INT_MAX 'select bit_count(cast (2147483647 as int));'                  // INT_MAX
    qt_bit_count_INT_IN 'select bit_count(cast (-2147483648 as int));'                  // INT_MIN
    qt_bit_count_INT64_MAX 'select bit_count(cast (9223372036854775807 as bigint));'    // INT64_MAX
    qt_bit_count_INT64_MIN 'select bit_count(cast (-9223372036854775808 as bigint));'   // INT64_MIN
    // INT128_MAX
    qt_bit_count_INT128_MAX """
        select bit_count(170141183460469231731687303715884105727),
               bit_count(cast (170141183460469231731687303715884105727 as largeint));
    """
    // INT128_MIN
    qt_bit_count_INT128_MIN """
        select  bit_count(-170141183460469231731687303715884105728),
                bit_count(cast (-170141183460469231731687303715884105728 as largeint));
    """

    qt_select "select bit_count(bit_shift_right(-1, 63)), bit_count(bit_shift_right(-1, 63));"

    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    qt_bit_count 'select bit_count(number) from numbers("number"="10");'
    qt_bit_count 'select bit_count(0);'
    qt_bit_count 'select bit_count(-1);'
    qt_bit_count 'select bit_count(cast (-1 as tinyint));'    // 8
    qt_bit_count 'select bit_count(cast (-1 as smallint));'   // 16
    qt_bit_count 'select bit_count(cast (-1 as int));'        // 32
    qt_bit_count 'select bit_count(cast (-1 as bigint));'     // 64
    qt_bit_count 'select bit_count(cast (-1 as largeint));'   // 128
    qt_bit_count_TINYINT_MAX 'select bit_count(cast (127 as tinyint));'                 // TINYINT_MAX
    qt_bit_count_TINYINT_MIN 'select bit_count(cast (-128 as tinyint));'                // TINYINT_MIN
    qt_bit_count_SMALLINT_MAX 'select bit_count(cast (32767 as smallint));'             // SMALLINT_MAX
    qt_bit_count_SMALLINT_MIN 'select bit_count(cast (-32768 as smallint));'            // SMALLINT_MIN
    qt_bit_count_INT_MAX 'select bit_count(cast (2147483647 as int));'                  // INT_MAX
    qt_bit_count_INT_IN 'select bit_count(cast (-2147483648 as int));'                  // INT_MIN
    qt_bit_count_INT64_MAX 'select bit_count(cast (9223372036854775807 as bigint));'    // INT64_MAX
    qt_bit_count_INT64_MIN 'select bit_count(cast (-9223372036854775808 as bigint));'   // INT64_MIN
    // INT128_MAX
    qt_bit_count_INT128_MAX """
        select bit_count(170141183460469231731687303715884105727),
               bit_count(cast (170141183460469231731687303715884105727 as largeint));
    """
    // INT128_MIN
    qt_bit_count_INT128_MIN """
        select  bit_count(-170141183460469231731687303715884105728),
                bit_count(cast (-170141183460469231731687303715884105728 as largeint));
    """

    qt_select "select bit_count(bit_shift_right(-1, 63)), bit_count(bit_shift_right(-1, 63));"

    qt_bitxor """
        select 2^127, -2^127;
    """
    qt_bitxor """
        select number, number^127, -number, (-number)^127 from numbers("number"="5") order by number;
    """
    qt_bitxor """
        select number, number^number, -number, (-number)^(-number) from numbers("number"="5") order by number
    """
}
