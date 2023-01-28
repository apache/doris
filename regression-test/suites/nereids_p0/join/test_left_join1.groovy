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

suite("test_left_join1", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 

    def tableName = "test_left_join1"
    sql """drop table if exists ${tableName}"""
    sql new File("""${context.file.parent}/ddl/test_left_join1.sql""").text

    sql """insert into ${tableName} values (1, 123),(2, 124),(3, 125),(4, 126);"""

    qt_select """ SELECT
                          *
                          FROM
                  ( SELECT f_key, f_value FROM ${tableName} ) a
                  LEFT JOIN ( SELECT f_key, f_value FROM ${tableName} ) b ON a.f_key = b.f_key
                  LEFT JOIN (
                          SELECT
                  *
                  FROM
                  ${tableName}
                  WHERE
                  f_key IN ( SELECT f_key FROM ${tableName} WHERE f_key IN ( SELECT f_key FROM ${tableName} WHERE f_value > 123 ) )
                  ) c ON a.f_key = c.f_key
                  ORDER BY
                  a.f_key; 
             """
}
