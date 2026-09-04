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

import org.apache.doris.regression.util.SqlUtils

suite("q02", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: false]) {
        assertFalse(getFeConfig("enable_variant_v2").toBoolean())
        String sqlText = $/
select * from test_predefine2 order by id limit 10;
select * from test_predefine2 where array_contains(cast(v1['array_int'] as array<int>), 1) order by id limit 4;
select * from test_predefine2 where array_contains(cast(v1['array_string'] as array<string>), 'b') order by id limit 4;
select * from test_predefine2 where array_contains(cast(v1['array_decimal'] as array<decimal>), 1.1) order by id limit 4;
select * from test_predefine2 where array_contains(cast(v1['array_datetime'] as array<datetime>), '2021-01-01 00:00:00') order by id limit 4;
select * from test_predefine2 where array_contains(cast(v1['array_datetimev2'] as array<datetimev2>), '2021-01-01 00:00:00') order by id limit 4;
select * from test_predefine2 where array_contains(cast(v1['array_date'] as array<date>), '2021-01-01') order by id limit 4;
select * from test_predefine2 where array_contains(cast(v1['array_datev2'] as array<datev2>), '2021-01-01') order by id limit 4;
-- select * from test_predefine2 where array_contains(cast(v1['array_ipv4'] as array<ipv4>), '127.0.0.1') order by id limit 4;
-- select * from test_predefine2 where array_contains(cast(v1['array_ipv6'] as array<ipv6>), 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe') order by id limit 4;
select * from test_predefine2 where cast(v1['array_float'] as array<float>)[1] >= 1.11111 order by id limit 4;
select * from test_predefine2 where array_contains(cast(v1['array_boolean'] as array<boolean>), 1) order by id limit 4;
select * from test_predefine2 where cast(v1['int_'] as int) = 11111122 order by id limit 4;
select * from test_predefine2 where cast(v1['string_'] as string) = '12111222113' order by id limit 4;
select * from test_predefine2 where cast(v1['decimal_'] as decimal) >= 188118222.011121933 order by id limit 4;
select * from test_predefine2 where cast(v1['datetime_'] as datetime) = '2022-01-01 11:11:11' order by id limit 4;
select * from test_predefine2 where cast(v1['datetimev2_'] as datetimev2(6)) = '2022-01-01 11:11:11.999999' order by id limit 4;
select * from test_predefine2 where cast(v1['date_'] as date) = '2022-01-01' order by id limit 4;
select * from test_predefine2 where cast(v1['datev2_'] as datev2) = '2022-01-01' order by id limit 4;
select * from test_predefine2 where cast(v1['ipv4_'] as ipv4) = '127.0.0.1' order by id limit 4;
select * from test_predefine2 where cast(v1['ipv6_'] as ipv6) = 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe' order by id limit 4;
select * from test_predefine2 where cast(v1['float_'] as float) >= 128.11 order by id limit 4;
select * from test_predefine2 where cast(v1['boolean_'] as boolean) = 1 order by id limit 4;
select * from test_predefine2 where cast(v1['varchar_'] as varchar) = 'hello world' order by id limit 4;
/$
        List<String> sqls = SqlUtils.splitAndGetNonEmptySql(sqlText)
        sqls.eachWithIndex { String statement, int index ->
            quickTest(index == 0 ? "q02" : "q02_${index + 1}", statement, false)
        }
    }
}
