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

suite("test_json_type_cast", "p0") {
    qt_sql1 "SELECT CAST(CAST(10 AS JSON) as INT)"
    qt_sql2 "SELECT CAST(CAST(102423 AS JSON) as TINYINT)"
    qt_sql3 "SELECT CAST(CAST(102423 AS JSON) as SMALLINT)"
    qt_sql4 "SELECT CAST(CAST(102400001234 AS JSON) as INT)"
    qt_sql5 "SELECT CAST(CAST(102400001234 AS JSON) as SMALLINT)"
    qt_sql6 "SELECT CAST(CAST(102400001234 AS JSON) as TINYINT)"
    qt_sql7 "SELECT CAST(CAST(102400001234 AS JSON) as BOOLEAN)"
    qt_sql8 "SELECT CAST(CAST(1000.1111 AS JSON) as INT)"
    qt_sql9 "SELECT CAST(CAST(1000.1111 AS JSON) as DOUBLE)"
    qt_sql10 "SELECT CAST(CAST(1000.1111 AS JSON) as BOOLEAN)"

    qt_sql11 """select cast('["CXO0N: 1045901740", "HMkTa: 1348450505", "44 HHD: 915015173", "j9WoJ: -1517316688"]' as json);"""
    qt_sql12 """select cast("111111" as json)"""
    qt_sql13 """select cast(111111 as json)"""
    qt_sql14 """select cast(1.1111 as json)"""

    qt_sql15 """select cast("+" as int);"""
    qt_sql16 """select cast("-" as int);"""
    qt_sql17 """select cast("a" as int);"""
    qt_sql18 """select cast("/" as int);"""
}