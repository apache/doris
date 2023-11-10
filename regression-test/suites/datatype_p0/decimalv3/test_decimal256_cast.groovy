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

suite("test_decimal256_cast") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    qt_decimal256_cast0 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""
    qt_decimal256_cast1 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast(-999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""
    qt_decimal256_cast2 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast(999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""
    qt_decimal256_cast3 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast(-999999999999999999999999999999999999999999999999999999999999999999.9999999999 as decimalv3(76,10));"""

    qt_decimal256_cast4 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast("999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""
    qt_decimal256_cast5 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = false) */
        cast("-999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""
    qt_decimal256_cast6 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast("999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""
    qt_decimal256_cast7 """SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
        cast("-999999999999999999999999999999999999999999999999999999999999999999.9999999999" as decimalv3(76,10));"""

}