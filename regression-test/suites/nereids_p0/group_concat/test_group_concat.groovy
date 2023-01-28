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

suite("test_group_concat") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    qt_select """
                SELECT group_concat(k6) FROM test_query_db.test where k6='false'
              """

    qt_select """
                SELECT group_concat(DISTINCT k6) FROM test_query_db.test where k6='false'
              """

    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar) order by abs(k2), k1) FROM test_query_db.baseall group by abs(k3) order by abs(k3)
              """
              
    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar), ":" order by abs(k2), k1) FROM test_query_db.baseall group by abs(k3) order by abs(k3)
              """
    qt_select """
                SELECT abs(k3), group_concat(distinct cast(abs(k2) as char) order by abs(k1), k2) FROM test_query_db.baseall group by abs(k3) order by abs(k3);
              """
    qt_select """
                SELECT abs(k3), group_concat(distinct cast(abs(k2) as char), ":" order by abs(k1), k2) FROM test_query_db.baseall group by abs(k3) order by abs(k3);
              """


    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar) order by abs(k2), k1) FROM test_query_db.baseall group by abs(k3) order by abs(k3)
              """

    qt_select """
                SELECT abs(k3), group_concat(cast(abs(k2) as varchar), ":" order by abs(k2), k1) FROM test_query_db.baseall group by abs(k3) order by abs(k3)
              """
    qt_select """
                SELECT abs(k3), group_concat(distinct cast(abs(k2) as char) order by abs(k1), k2) FROM test_query_db.baseall group by abs(k3) order by abs(k3);
              """
    qt_select """
                SELECT abs(k3), group_concat(distinct cast(abs(k2) as char), ":" order by abs(k1), k2) FROM test_query_db.baseall group by abs(k3) order by abs(k3);
              """
}
