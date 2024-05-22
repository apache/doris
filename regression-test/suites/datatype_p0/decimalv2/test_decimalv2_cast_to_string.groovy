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

suite("test_decimalv2_cast_to_string", "nonConcurrent") {
    def config_row = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'disable_decimalv2'; """
    String old_value1 = config_row[0][1]
    config_row = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_decimal_conversion'; """
    String old_value2 = config_row[0][1]

    sql """
        admin set frontend config("enable_decimal_conversion" = "false");
    """
    sql """
        admin set frontend config("disable_decimalv2" = "false");
    """

    sql """
        drop table if exists decimalv2_cast_to_string_test;
    """
    sql """
        create table decimalv2_cast_to_string_test (k1 decimalv2(27,9)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into decimalv2_cast_to_string_test  values (null), (0), (1), (1.123), (0.1), (0.000000001), (999999999999999999), (999999999999999999.999999999);
    """
    sql """
        insert into decimalv2_cast_to_string_test  values (-1), (-1.123), (-0.1), (-0.000000001), (-999999999999999999), (-999999999999999999.999999999);
    """

    qt_cast1 """
        select k1, cast(k1 as varchar) from decimalv2_cast_to_string_test order by 1;
    """

    sql """
        drop table if exists decimalv2_cast_to_string_test2;
    """
    sql """
        create table decimalv2_cast_to_string_test2 (k1 decimalv2(10,3)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into decimalv2_cast_to_string_test2 values (null), (0), (0.1), (0.001), (1), (1.1), (1.123), (1.123456789), (9999999), (9999999.001), (9999999.999);
    """
    sql """
        insert into decimalv2_cast_to_string_test2 values (-0.1), (-0.001), (-1), (-1.1), (-1.123), (-1.123456789), (-9999999), (-9999999.001), (-9999999.999);
    """

    qt_cast2 """
        select k1, cast(k1 as varchar) from decimalv2_cast_to_string_test2 order by 1;
    """

    // restore disable_decimalv2 to old_value
    sql """ ADMIN SET FRONTEND CONFIG ("disable_decimalv2" = "${old_value1}"); """

    // restore enable_decimal_conversion to old_value
    sql """ ADMIN SET FRONTEND CONFIG ("enable_decimal_conversion" = "${old_value2}"); """
}