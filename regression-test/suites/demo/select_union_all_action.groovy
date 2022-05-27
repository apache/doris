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

suite("select_union_all_action", "demo") {
    // 3 rows and 1 column
    def rows = [3, 1, 10]
    order_qt_select_union_all1 """
            select c1
            from
            (
              ${selectUnionAll(rows)}
            ) a
            """

    // 3 rows and 2 columns
    rows = [[1, "123"], [2, null], [0, "abc"]]
    order_qt_select_union_all2 """
             select c1, c2
             from
             (
                ${selectUnionAll(rows)}
             ) b
             """
}