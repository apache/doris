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

suite("load") {
    multi_sql """
        drop table if exists test_decimal_mul_overflow1;
        CREATE TABLE `test_decimal_mul_overflow1` (
            `f1` decimal(20,5) NULL,
            `f2` decimal(21,6) NULL
        )DISTRIBUTED BY HASH(f1)
        PROPERTIES("replication_num" = "1");
        insert into test_decimal_mul_overflow1 values(999999999999999.12345,999999999999999.123456);
    """

    multi_sql """
        drop table if exists t_decimalv3;
        create table t_decimalv3(a decimal(38,9),b decimal(38,10))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        insert into t_decimalv3 values(1.012345678,1.0123456789);
    """

    multi_sql """
        drop table if exists t_decimalv3_for_compare;
        create table t_decimalv3_for_compare(a decimal(38,9),b decimal(38,10))
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
        insert into t_decimalv3_for_compare values(1.012345678,1.0123456781);
    """
}