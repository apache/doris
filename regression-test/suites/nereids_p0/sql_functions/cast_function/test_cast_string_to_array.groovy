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

suite("test_cast_string_to_array") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    // cast string to array<int>
    // Nereids does't support array function
    // qt_sql """ select cast ("[1,2,3]" as array<int>) """

    // cast string to array<string>
    // Nereids does't support array function
    // qt_sql """ select cast ("['a','b','c']" as array<string>) """

    // cast string to array<double>
    // Nereids does't support array function
    // qt_sql """ select cast ("[1.34,2.001]" as array<double>) """

    // cast string to array<decimal>
    // Nereids does't support array function
    // qt_sql """ select cast ("[1.34,2.001]" as array<decimal>) """

    // cast string to array<date>
    // Nereids does't support array function
    // qt_sql """ select cast ("[2022-09-01]" as array<date>) """

    // cast empty value
    // Nereids does't support array function
    // qt_sql """ select cast ("[1,2,3,,,]" as array<int>) """
    // Nereids does't support array function
    // qt_sql """ select cast ("[a,b,c,,,]" as array<string>) """
    // Nereids does't support array function
    // qt_sql """ select cast ("[1.34,2.01,,,]" as array<decimal>) """
    // Nereids does't support array function
    // qt_sql """ select cast ("[2022-09-01,,]" as array<date>) """
}
