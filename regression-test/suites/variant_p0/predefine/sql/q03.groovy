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

// Reads back the DECIMAL and big-integer Schema Template documents that load.groovy loads into
// test_predefine_decimal and test_predefine_bigint. Each document is there three times, loaded by
// INSERT ... SELECT parse_to_variant (its id), INSERT ... SELECT try_parse_to_variant (1000 + id)
// and Stream Load (2000 + id), and some a fourth time by INSERT ... VALUES (3000 + id): the rows
// of one document must read alike.
suite("q03", "p0,nonConcurrent") {
        String sqlText = $/
select id, cast(v as string) from test_predefine_decimal order by id;
select id, cast(v['d38_0'] as decimal(38, 0)), cast(v['d38_38'] as decimal(38, 38)), cast(v['d10_2'] as decimal(10, 2)) from test_predefine_decimal order by id;
select id, cast(v['d38_0'] as string), cast(v['d38_38'] as string), cast(v['d10_2'] as string), cast(v['other'] as string) from test_predefine_decimal order by id;
select id, cast(v['prices'] as array<decimal(20, 4)>), cast(v['prices'] as string) from test_predefine_decimal where id % 1000 = 18 order by id;
select id, cast(v['amount'] as string), cast(v['amount_due'] as string), cast(v['total'] as decimal(10, 2)), cast(v['total_due'] as string) from test_predefine_decimal where id % 1000 = 19 order by id;
select id, cast(v['item']['price'] as decimal(10, 2)), cast(v['item'] as string), cast(v['lines'] as string) from test_predefine_decimal where id % 1000 in (20, 21) order by id;
select id from test_predefine_decimal where cast(v['d38_0'] as decimal(38, 0)) = 12345678901234567890123456789012345678 order by id;
select id from test_predefine_decimal where cast(v['d38_38'] as decimal(38, 38)) = 0.12345678901234567890123456789012345678 order by id;
select id from test_predefine_decimal where cast(v['d10_2'] as decimal(10, 2)) = 12345678.99 order by id;
select id, cast(v as string) from test_predefine_bigint order by id;
select id, cast(v['id'] as largeint), cast(v['n'] as bigint), cast(v['obj']['id'] as largeint), cast(v['big_a'] as largeint), cast(v['big_b'] as largeint) from test_predefine_bigint order by id;
select id, cast(v['id'] as string), cast(v['n'] as string), cast(v['other'] as string) from test_predefine_bigint order by id;
select id, cast(v['ids'] as array<largeint>), cast(v['ids'] as string) from test_predefine_bigint where id % 1000 in (14, 15, 43) order by id;
select id from test_predefine_bigint where cast(v['id'] as largeint) = 18446744073709551616 order by id;
select id from test_predefine_bigint where cast(v['id'] as largeint) = 170141183460469231731687303715884105727 order by id;
select id from test_predefine_bigint where cast(v['id'] as largeint) = -170141183460469231731687303715884105727 order by id;
select id, cast(v['id'] as largeint) - 18446744073709551615 from test_predefine_bigint where id % 1000 = 4 order by id;
select id, v is null from test_predefine_bigint where id % 1000 between 31 and 35 order by id;
/$
        List<String> sqls = SqlUtils.splitAndGetNonEmptySql(sqlText)
        sqls.eachWithIndex { String statement, int index ->
            quickTest(index == 0 ? "q03" : "q03_${index + 1}", statement, false)
        }

}
