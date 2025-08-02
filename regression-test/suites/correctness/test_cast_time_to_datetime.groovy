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

suite("test_cast_time_to_datetime") {
    qt_sql "select cast(cast('0000-01-01 12:12:12' as datetime) as time);"
    qt_sql "select cast(cast('2000-02-03 12:12:12' as datetime) as time);"
    qt_sql "select cast(cast('2000-02-03 12:12:12.123456' as datetime(6)) as time(4));"
    qt_sql "select cast(cast(cast('2020-12-12 12:12:12' as time) as datetime) as time);"
    qt_sql "select time_to_sec(cast('2002-05-30 10:10:20' as datetime));"
    def res = sql "select date_format(cast(cast('2000-02-03 12:12:12.123456' as datetime(6)) as time(4)), '%b %e %Y %l:%i%p');"
    // check final 7 char of res[0][0] is 12:12PM
    assertEquals("12:12PM", res[0][0].substring(res[0][0].length() - 7))
}