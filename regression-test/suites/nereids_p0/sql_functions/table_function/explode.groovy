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

suite("explode") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    // Nereids does't support array function
    // qt_explode """ select e1 from (select 1 k1) as t lateral view explode([1,2,3]) tmp1 as e1; """
    // Nereids does't support array function
    // qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([1,2,3]) tmp1 as e1; """

    // array is null
    // Nereids does't support array function
    // qt_explode """ select e1 from (select 1 k1) as t lateral view explode(null) tmp1 as e1; """
    // Nereids does't support array function
    // qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer(null) tmp1 as e1; """

    // array is empty
    // Nereids does't support array function
    // qt_explode """ select e1 from (select 1 k1) as t lateral view explode([]) tmp1 as e1; """
    // Nereids does't support array function
    // qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([]) tmp1 as e1; """

    // array with null elements
    // Nereids does't support array function
    // qt_explode """ select e1 from (select 1 k1) as t lateral view explode([null,1,null]) tmp1 as e1; """
    // Nereids does't support array function
    // qt_explode_outer """ select e1 from (select 1 k1) as t lateral view explode_outer([null,1,null]) tmp1 as e1; """
}
