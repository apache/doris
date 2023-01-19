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

suite("test_nereids_utf8_operation") {
    sql "use test_query_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    order_qt_sql_1 """
        SELECT k1 AS 测试 FROM  test;
    """

    order_qt_sql_2 """
        SELECT k1 AS テスト FROM test;        
    """

    order_qt_sql_3 """
        SELECT k1 AS Å FROM test;
    """

    qt_sql_4 """
        SELECT SUBSTRING("dddd编", 1, 3) "测试";
    """

    qt_sql_5 """
         SELECT SUBSTRING("dddd编", 1, 3) AS "测试";
    """
}