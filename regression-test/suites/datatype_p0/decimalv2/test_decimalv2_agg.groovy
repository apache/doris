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

suite("test_decimalv2_agg", "nonConcurrent") {

    sql """
        admin set frontend config("enable_decimal_conversion" = "false");
    """

    sql """
        drop table if exists test_decimalv2_agg;
    """
    sql """
        create table test_decimalv2_agg (
            k1 decimalv2(27,9)
        )distributed by hash(k1)
        properties("replication_num"="1");
    """
    sql """
        insert into test_decimalv2_agg values
            (123456789012345678.901234567),
            (-123456789012345678.901234567),
            (0.012345679),
            (NULL);
    """
    qt_decimalv2_min " select min(k1) from test_decimalv2_agg; "
    qt_decimalv2_max " select max(k1) from test_decimalv2_agg; "
    qt_decimalv2_count " select count(k1) from test_decimalv2_agg; "
    qt_decimalv2_sum " select sum(k1) from test_decimalv2_agg; "
    qt_decimalv2_avg " select avg(k1) from test_decimalv2_agg; "

    // test overflow
    sql """
        drop table if exists test_decimalv2_agg;
    """
    sql """
        create table test_decimalv2_agg (
            k1 decimalv2(27,9)
        )distributed by hash(k1)
        properties("replication_num"="1");
    """
    sql """
        insert into test_decimalv2_agg values
            (999999999999999999.999999999),
            (0.000000001),
            (NULL);
    """
    qt_decimalv2_sum2 " select sum(k1) from test_decimalv2_agg; "

}