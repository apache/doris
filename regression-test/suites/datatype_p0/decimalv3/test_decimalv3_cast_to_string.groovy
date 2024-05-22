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

suite("test_decimalv3_cast_to_string") {
    sql """
        drop table if exists decimalv3_cast_to_string_test;
    """
    sql """
        create table decimalv3_cast_to_string_test (k1 decimalv3(38,6)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into decimalv3_cast_to_string_test values (null), (0), (1), (1.123), (0.1), (0.000001), (99999999999999999999999999999999), ("99999999999999999999999999999999.000001"), ("99999999999999999999999999999999.999999");
    """
    sql """
        insert into decimalv3_cast_to_string_test values (-1), (-1.123), (-0.1), (-0.000001), (-99999999999999999999999999999999), ("-99999999999999999999999999999999.000001"), ("-99999999999999999999999999999999.999999");
    """

    qt_cast1 """
        select k1, cast(k1 as varchar) from decimalv3_cast_to_string_test order by 1;
    """
}