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

// There once exists a bug that when passing a too long string
// along with an empty one to find_in_set, BE 

suite("test_find_in_set") {

    qt_select """
        select find_in_set(
        cast(regression_test_tpch_sf1_p2.orders.`O_COMMENT` as varchar),
        cast(BITMAP_TO_STRING(
        cast(BITMAP_EMPTY() as bitmap)) as varchar)) from regression_test_tpch_sf1_p2.orders limit 1
    """

}