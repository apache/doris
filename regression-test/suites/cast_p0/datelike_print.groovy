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

suite("datelike_print") {
    qt_sql1 "SELECT CAST(CAST('2024-06-15' AS DATE) AS STRING);"
    qt_sql2 "SELECT CAST(CAST('2024-06-15 12:34:56' AS DATETIME) AS STRING);"
    qt_sql3 "SELECT CAST(CAST('2024-06-15 12:34:56' AS DATETIME(6)) AS STRING);"
    qt_sql4 "SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME) AS STRING);"
    qt_sql5 "SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME(3)) AS STRING);"
    qt_sql6 "SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME(4)) AS STRING);"
    qt_sql7 "SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME(6)) AS STRING);"
    qt_sql8 "SELECT CAST(CAST('0' AS TIME) AS STRING);"
    qt_sql9 "SELECT CAST(CAST('0' AS TIME(6)) AS STRING);"
    qt_sql10 "SELECT CAST(CAST('23:59:59' AS TIME(6)) AS STRING);"
    qt_sql11 "SELECT CAST(CAST('123:00:00' AS TIME(6)) AS STRING);"
    qt_sql12 "SELECT CAST(CAST('800:12:34.56' AS TIME(6)) AS STRING);"

    testFoldConst("SELECT CAST(CAST('2024-06-15' AS DATE) AS STRING);")
    testFoldConst("SELECT CAST(CAST('2024-06-15 12:34:56' AS DATETIME) AS STRING);")
    testFoldConst("SELECT CAST(CAST('2024-06-15 12:34:56' AS DATETIME(6)) AS STRING);")
    testFoldConst("SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME) AS STRING);")
    testFoldConst("SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME(3)) AS STRING);")
    testFoldConst("SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME(4)) AS STRING);")
    testFoldConst("SELECT CAST(CAST('2024-06-15 12:34:56.123' AS DATETIME(6)) AS STRING);")
    testFoldConst("SELECT CAST(CAST('0' AS TIME) AS STRING);")
    testFoldConst("SELECT CAST(CAST('0' AS TIME(6)) AS STRING);")
    testFoldConst("SELECT CAST(CAST('23:59:59' AS TIME(6)) AS STRING);")
    testFoldConst("SELECT CAST(CAST('123:00:00' AS TIME(6)) AS STRING);")
    testFoldConst("SELECT CAST(CAST('800:12:34.56' AS TIME(6)) AS STRING);")
}