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

suite("test_with") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db"
    //Basic WITH
    qt_select """
                select 1 from (with w as (select 1 from baseall 
                where exists (select 1 from baseall)) select 1 from w ) tt
              """
    qt_select """
                WITH q1(x,y) AS (SELECT 1,2)
                SELECT * FROM q1, q1 AS q2;
              """
 
    qt_select """
                WITH outermost(x) AS (
                  SELECT 1
                  UNION (WITH innermost as (SELECT 2)
                        SELECT * FROM innermost
                        UNION SELECT 3)
                )
                SELECT x FROM outermost order by 1;
              """
    qt_select """
                WITH outermost(x) AS (
                  SELECT 1
                  UNION (WITH innermost as (SELECT 2)
                        SELECT * FROM innermost
                        UNION SELECT 3)
                )
                SELECT * FROM outermost ORDER BY 1;
              """


}
