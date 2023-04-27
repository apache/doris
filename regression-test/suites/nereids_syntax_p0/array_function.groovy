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

suite("array_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // test {
    //     sql "select array(), array(null), array(1), array('abc'), array(null, 1), array(1, null)"
    //     result([["[]", "[NULL]", "[1]", "['abc']", "[NULL, 1]", "[1, NULL]"]])
    // }

    // test {
    //     sql "select array(), array('a'), array(number, 'a') from numbers('number'='3')"
    //     result([
    //         ["[]", "['a']", "['0', 'a']"],
    //         ["[]", "['a']", "['1', 'a']"],
    //         ["[]", "['a']", "['2', 'a']"]
    //     ])
    // }

    // test {
    //     sql """select
    //                  array_min(array(5, 4, 3, 2, 1, null)),
    //                  array_join(array(5, 4, 3, 2, 1, null), ','),
    //                  array_union(array(1, 2, 3), array(4.0, 5.0, 6.1))"""
    //     result([[1, "5,4,3,2,1", "[1, 2, 3, 4, 5, 6.1]"]])
    // }
}
