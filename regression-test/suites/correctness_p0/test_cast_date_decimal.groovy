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

suite("test_cast_date_decimal") {
    qt_sql1 """
        select cast('2020-02-02' as date ) between cast('2020-02-02' as date ) and cast('2020-02-02' as date ) + 1.0;
    """

    qt_sql2 """
        select cast('2024-12-12' as date);
    """

    qt_sql3 """
        select cast('2024.12.12' as date);
    """

    qt_sql4 """
        select cast('24.12.12' as date);
    """

    qt_sql5 """
        select cast('123.123' as date);
    """

    qt_sql6 """
        select cast('0000-02-29' as date), cast('0000-02-29' as datetime), cast('00000229' as date), cast('0000-02-29 12:12:12.123' as datetime);
    """

    qt_sql7 """
        select /*+SET_VAR(debug_skip_fold_constant=true)*/ cast('0000-02-29' as date), cast('0000-02-29' as datetime), cast('00000229' as date), cast('0000-02-29 12:12:12.123' as datetime);
    """
}
