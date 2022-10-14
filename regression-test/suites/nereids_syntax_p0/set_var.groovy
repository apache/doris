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

suite("set_var") {
    // enable nereids and vectorized engine
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // test single set_var
    sql "select /*+ SET_VAR(query_timeout=1800) */ * from supplier limit 10"

    // test multi set_var, comma between hints is optional
    sql """ select
        /*+
            SET_VAR(query_timeout=1800, enable_vectorized_engine=true)
            SET_VAR(query_timeout=1800, enable_vectorized_engine=true),
            SET_VAR(query_timeout=1800, enable_vectorized_engine=true)
        */
        *
        from supplier
        limit 10
        """

    // test whether the hints are effective
    // set an invalid parameter, and throw an exception
    test {
        sql "select /*+SET_VAR(runtime_filter_type=10000)*/ * from supplier limit 10"
        exception "Unexpected exception: Can not set session variable"
    }
}
