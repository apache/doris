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

suite("test_alias_function") {

    sql """DROP FUNCTION IF EXISTS mesh_udf_test1(INT,INT)"""
    sql """CREATE ALIAS FUNCTION IF NOT EXISTS mesh_udf_test1(INT,INT) WITH PARAMETER(n,d) AS ROUND(1+floor(n/d));"""
    qt_sql1 """select mesh_udf_test1(1,2);"""

    sql """DROP FUNCTION IF EXISTS mesh_udf_test2(INT,INT)"""
    sql """CREATE ALIAS FUNCTION IF NOT EXISTS mesh_udf_test2(INT,INT) WITH PARAMETER(n,d) AS add(1,floor(divide(n,d)))"""
    qt_sql1 """select mesh_udf_test2(1,2);"""
}
