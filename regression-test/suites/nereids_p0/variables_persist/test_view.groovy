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

suite("test_view") {

    multi_sql """drop table if exists test_decimal_mul_overflow1;
    CREATE TABLE `test_decimal_mul_overflow1` (
    `f1` decimal(20,5) NULL,
    `f2` decimal(21,6) NULL
    )DISTRIBUTED BY HASH(f1)
    PROPERTIES("replication_num" = "1");
    insert into test_decimal_mul_overflow1 values(999999999999999.12345,999999999999999.123456);"""

    // 这个结果是完整的结果，超出了38个精度
    // 999999999999998246906000000000.76833464320
    // 这个是截断到38个精度的结果
    // 999999999999998246906000000000.76833464

    // 打开enable256,创建视图;
    sql "set enable_decimal256=true;"
    sql "drop view if exists v_test_decimal_mul_overflow1;"
    sql """create view v_test_decimal_mul_overflow1 as select f1,f2,f1*f2 multi from test_decimal_mul_overflow1;"""

    // 关闭enable256,进行查询，预期结果是multi超出38个精度的结果, multi+1仍然是38个精度
    sql "set enable_decimal256=false;"

    // expect column multi scale is 11:999999999999998246906000000000.76833464320 instead of 8: 999999999999998246906000000000.76833464
    qt_scale_is_11 "select multi from v_test_decimal_mul_overflow1;"
    // expect column c1 scale is 8: 999999999999998246906000000000.76833464
//    qt_scale_is_8 "select multi+1 c1 from v_test_decimal_mul_overflow1;"

}