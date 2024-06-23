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

suite("test_array_functions_array_join", "p0") {
    // ============= array join =========
    qt_old_sql "SELECT 'array-join';"
    // only support array_join with two arguments
    // order_qt_sql "SELECT array_join(['Hello', 'World']);"
    order_qt_old_sql "SELECT array_join(['Hello', 'World'], ', ');"
    // order_qt_sql "SELECT array_join([]);"
    // order_qt_sql "SELECT array_join(array_range(number)) FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join(array_range(number), '') FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join(array_range(number), ',') FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join(array_range(number % 4), '_') FROM numbers LIMIT 10;"
    order_qt_old_sql "SELECT array_join([Null, 'hello', Null, 'world', Null, 'xyz', 'def', Null], ';');"
    order_qt_old_sql "SELECT array_join([1, 23, 456], ';');"
    order_qt_old_sql "SELECT array_join([Null, 1, Null, 23, Null, 456, Null], ';');"
    // array with ip type make mistake
    // old planner
//    mysql> select array(cast('127.0.0.1' as ipv4), Null);
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // array_join mistake
    // old planner
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // nereids
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]execute failed or unsupported types for function array_join(Array(Nullable(IPv4)),String,)

    // nereids
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"

    // ============= array join =========
    qt_nereid_sql "SELECT 'array-join';"
    // only support array_join with two arguments
    // order_qt_sql "SELECT array_join(['Hello', 'World']);"
    order_qt_nereid_sql "SELECT array_join(['Hello', 'World'], ', ');"
    // order_qt_sql "SELECT array_join([]);"
    // order_qt_sql "SELECT array_join(array_range(number)) FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join(array_range(number), '') FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join(array_range(number), ',') FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join(array_range(number % 4), '_') FROM numbers LIMIT 10;"
    order_qt_nereid_sql "SELECT array_join([Null, 'hello', Null, 'world', Null, 'xyz', 'def', Null], ';');"
    order_qt_nereid_sql "SELECT array_join([1, 23, 456], ';');"
    order_qt_nereid_sql "SELECT array_join([Null, 1, Null, 23, Null, 456, Null], ';');"
    // array with ip type make mistake
    // old planner
//    mysql> select array(cast('127.0.0.1' as ipv4), Null);
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // array_join mistake
    // old planner
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]Conversion from IPv4 to Decimal(27, 9) is not supported
    // nereids
//    mysql> select array_join(array(cast('127.0.0.1' as ipv4), Null, cast('1.0.0.1' as ipv4)), ',');
//    ERROR 1105 (HY000): errCode = 2, detailMessage = (172.21.16.12)[CANCELLED]execute failed or unsupported types for function array_join(Array(Nullable(IPv4)),String,)

}
