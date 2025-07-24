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

#include "cast_to_decimal.h"

#include <fstream>
#include <memory>

#include "cast_test.h"
#include "common/exception.h"
#include "olap/olap_common.h"
#include "testutil/test_util.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"

namespace doris::vectorized {

/*
TODO, fix:
mysql> select cast('+000999998000.5e-3' as Decimal(9, 3));
+---------------------------------------------+
| cast('+000999998000.5e-3' as Decimal(9, 3)) |
+---------------------------------------------+
|                                  999998.000 |
+---------------------------------------------+
1 row in set (8.54 sec)

// expected result: 9999999999999999.0
select cast('+0009999999999999999040000000.e-9' as decimal(18,1));
+------------------------------------------------------------+
| cast('+0009999999999999999040000000.e-9' as decimal(18,1)) |
+------------------------------------------------------------+
|                                        99999999999999999.9 |
+------------------------------------------------------------+
1 row in set (0.65 sec)

PG:
e1
postgres=# select cast('1e' as decimal(18,6));
ERROR:  invalid input syntax for type numeric: "1e"
LINE 1: select cast('1e' as decimal(18,6));

postgres=# select cast('.e1' as decimal(18,6));
ERROR:  invalid input syntax for type numeric: ".e1"
LINE 1: select cast('.e1' as decimal(18,6));
                    ^
postgres=# select cast('.1e1' as decimal(18,6));
 numeric  
----------
 1.000000
(1 row)

edge cases:
1000 digits
postgres=# select cast('1151111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111e-999' as decimal(38, 1));
 numeric 
---------
     1.2
(1 row)

MySQL 8.0
ysql> select cast('1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111e-980' as decimal(38, 1));
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| cast('111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                                                                                                                         9999999999999999999999999999999999999.9 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set, 3 warnings (0.00 sec)

*/
TEST_F(FunctionCastToDecimalTest, test_from_string_invalid_input) {
    int table_index = 0;
    from_string_invalid_input_test_func<Decimal32>(9, 3, table_index++);
}
TEST_F(FunctionCastToDecimalTest, test_from_bool) {
    from_bool_test_func<Decimal32>(9, 0);
    from_bool_test_func<Decimal32>(9, 1);
    from_bool_test_func<Decimal32>(9, 3);
    from_bool_test_func<Decimal32>(9, 8);

    from_bool_test_func<Decimal64>(18, 0);
    from_bool_test_func<Decimal64>(18, 1);
    from_bool_test_func<Decimal64>(18, 9);
    from_bool_test_func<Decimal64>(18, 17);

    from_bool_test_func<Decimal128V2>(27, 9);
    // from_bool_test_func<Decimal128V2, 27, 1>();
    // from_bool_test_func<Decimal128V2, 27, 13>();
    // from_bool_test_func<Decimal128V2, 27, 26>();

    from_bool_test_func<Decimal128V3>(38, 0);
    from_bool_test_func<Decimal128V3>(38, 1);
    from_bool_test_func<Decimal128V3>(38, 19);
    from_bool_test_func<Decimal128V3>(38, 37);

    from_bool_test_func<Decimal256>(76, 0);
    from_bool_test_func<Decimal256>(76, 1);
    from_bool_test_func<Decimal256>(76, 38);
    from_bool_test_func<Decimal256>(76, 75);
}

TEST_F(FunctionCastToDecimalTest, test_from_bool_overflow) {
    from_bool_overflow_test_func<Decimal32>();
    from_bool_overflow_test_func<Decimal64>();
    from_bool_overflow_test_func<Decimal128V2>();
    from_bool_overflow_test_func<Decimal128V3>();
    from_bool_overflow_test_func<Decimal256>();
}
} // namespace doris::vectorized