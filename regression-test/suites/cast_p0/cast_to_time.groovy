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

suite("cast_to_time") {
    sql "set debug_skip_fold_constant = true"
//TODO: after we finished cast refactor, we can parse microseconds
qt_sql """ select cast("1" as time(6)) """
qt_sql """ select cast("123" as time(6)) """
qt_sql """ select cast("2005959.12" as time(6)) """
qt_sql """ select cast("0.12" as time(6)) """
qt_sql """ select cast("00:00:00.12" as time(6)) """
qt_sql """ select cast("123." as time(6)) """
qt_sql """ select cast("123.0" as time(6)) """
qt_sql """ select cast("123.123" as time(6)) """
qt_sql """ select cast("-1" as time(6)) """
qt_sql """ select cast("-800:05:05" as time(6)) """
qt_sql """ select cast("-991213.56" as time(6)) """
qt_sql """ select cast("80302.9999999" as time(6)) """
qt_sql """ select cast("5656.3000000009" as time(6)) """
qt_sql """ select cast("5656.3000007001" as time(6)) """
qt_sql """ select cast("   1   " as time(6)) """
qt_sql """ select cast(".123" as time(6)) """
qt_sql """ select cast(":12:34" as time(6)) """
qt_sql """ select cast("12-34:56.1" as time(6)) """
qt_sql """ select cast("12 : 34 : 56" as time(6)) """
qt_sql """ select cast("76" as time(6)) """
qt_sql """ select cast("200595912" as time(6)) """
qt_sql """ select cast("8385959.9999999" as time(6)) """

qt_sql """ select cast("1" as time(3)) """
qt_sql """ select cast("123" as time(3)) """
qt_sql """ select cast("2005959.12" as time(3)) """
qt_sql """ select cast("0.12" as time(3)) """
qt_sql """ select cast("00:00:00.12" as time(3)) """
qt_sql """ select cast("123." as time(3)) """
qt_sql """ select cast("123.0" as time(3)) """
qt_sql """ select cast("123.123" as time(3)) """
qt_sql """ select cast("-1" as time(3)) """
qt_sql """ select cast("-800:05:05" as time(3)) """
qt_sql """ select cast("-991213.56" as time(3)) """
qt_sql """ select cast("80302.9999999" as time(3)) """
qt_sql """ select cast("5656.3000000009" as time(3)) """
qt_sql """ select cast("5656.3000007001" as time(3)) """
qt_sql """ select cast("   1   " as time(3)) """
qt_sql """ select cast(".123" as time(3)) """
qt_sql """ select cast(":12:34" as time(3)) """
qt_sql """ select cast("12-34:56.1" as time(3)) """
qt_sql """ select cast("12 : 34 : 56" as time(3)) """
qt_sql """ select cast("76" as time(3)) """
qt_sql """ select cast("200595912" as time(3)) """
qt_sql """ select cast("8385959.9999999" as time(3)) """

qt_sql """ select cast("1" as time(0)) """
qt_sql """ select cast("123" as time(0)) """
qt_sql """ select cast("2005959.12" as time(0)) """
qt_sql """ select cast("0.12" as time(0)) """
qt_sql """ select cast("00:00:00.12" as time(0)) """
qt_sql """ select cast("123." as time(0)) """
qt_sql """ select cast("123.0" as time(0)) """
qt_sql """ select cast("123.123" as time(0)) """
qt_sql """ select cast("-1" as time(0)) """
qt_sql """ select cast("-800:05:05" as time(0)) """
qt_sql """ select cast("-991213.56" as time(0)) """
qt_sql """ select cast("80302.9999999" as time(0)) """
qt_sql """ select cast("5656.3000000009" as time(0)) """
qt_sql """ select cast("5656.3000007001" as time(0)) """
qt_sql """ select cast("   1   " as time(0)) """
qt_sql """ select cast(".123" as time(0)) """
qt_sql """ select cast(":12:34" as time(0)) """
qt_sql """ select cast("12-34:56.1" as time(0)) """
qt_sql """ select cast("12 : 34 : 56" as time(0)) """
qt_sql """ select cast("76" as time(0)) """
qt_sql """ select cast("200595912" as time(0)) """
qt_sql """ select cast("8385959.9999999" as time(0)) """

qt_sql """ select cast(cast(123456 as int) as time(3)) """
qt_sql """ select cast(cast(-123456 as int) as time(3)) """
qt_sql """ select cast(cast(123 as int) as time(3)) """
qt_sql """ select cast(cast(6.99999 as int) as time(3)) """
qt_sql """ select cast(cast(-0.99 as int) as time(3)) """
qt_sql """ select cast(cast(8501212 as int) as time(3)) """
qt_sql """ select cast(cast(20001212 as int) as time(3)) """
qt_sql """ select cast(cast(9000000 as int) as time(3)) """
qt_sql """ select cast(cast(67 as int) as time(3)) """

qt_sql """ select cast(cast(123456 as double) as time(3)) """
qt_sql """ select cast(cast(-123456 as double) as time(3)) """
qt_sql """ select cast(cast(123 as double) as time(3)) """
qt_sql """ select cast(cast(6.99999 as double) as time(3)) """
qt_sql """ select cast(cast(-0.99 as double) as time(3)) """
qt_sql """ select cast(cast(8501212 as double) as time(3)) """
qt_sql """ select cast(cast(20001212 as double) as time(3)) """
qt_sql """ select cast(cast(9000000 as double) as time(3)) """
qt_sql """ select cast(cast(67 as double) as time(3)) """

///FIXME: not support now
// qt_sql """ select cast(cast(123456 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(-123456 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(123 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(6.99999 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(-0.99 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(8501212 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(20001212 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(9000000 as decimal(27, 9)) as time(3)) """
// qt_sql """ select cast(cast(67 as decimal(27, 9)) as time(3)) """

// qt_sql """ select cast(cast("2012-02-05 12:12:12.123456" as datetime(6)) as time(4)) """

qt_sql "select cast('11:12:13.123456' as time) = cast('11:12:13.12' as time)"

    sql "set debug_skip_fold_constant = false"
qt_sql """ select cast("100:10:10.123456" as time(3)) """
qt_sql """ select cast("-100:10:10.123456" as time(3)) """
qt_sql """ select cast("100:10:10.12345699999" as time(3)) """
qt_sql """ select cast("100:10:10.12345699999" as time(6)) """
qt_sql """ select cast("100:10:10.9999999999" as time(6)) """
qt_sql """ select cast("x:10:10.123456" as time(3)) """
qt_sql """ select cast("-900:10:10.123456" as time(3)) """
}
