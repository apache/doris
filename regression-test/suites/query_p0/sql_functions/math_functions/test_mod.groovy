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

suite("test_mod") {
    def tableName = "test_mod"
    sql "set enable_fold_constant_by_be = false;"
    sql """DROP TABLE IF EXISTS `test_mod`"""
    sql """ CREATE TABLE `test_mod` (
        `k1` int NULL COMMENT "用户id",
        `k2` bigint COMMENT "数据灌入日期时间",
        `k3` int COMMENT "数据灌入日期时间")
        DUPLICATE KEY(`k1`) DISTRIBUTED BY HASH(`k1`)
        PROPERTIES ( "replication_num" = "1" ); """

    sql """ insert into `test_mod` values(1,2,3); """
    sql """ insert into `test_mod` values(-2147483648,4,-1); """
    sql """ insert into `test_mod` values(5,-9223372036854775808,-1); """

    qt_sql """
        SELECT * from test_mod order by 1;
    """

    test {
        sql "select mod(-2147483648,-1); "
        exception "Division of minimal signed number by minus one is an undefined"
    }
    test {
        sql "select mod(-9223372036854775808,-1); "
        exception "Division of minimal signed number by minus one is an undefined"
    }
    test {
        sql "select pmod(-9223372036854775808,-1); "
        exception "Division of minimal signed number by minus one is an undefined"
    }
}
