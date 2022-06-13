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

suite("test_union", "query") {
    order_qt_select "select k1, k2 from test_query_db.baseall union select k2, k3 from test_query_db.test"
    order_qt_select "select k2, count(k1) from ((select k2, avg(k1) k1 from test_query_db.baseall group by k2) union all (select k2, count(k1) k1 from test_query_db.test group by k2) )b group by k2 having k2 > 0 order by k2;"
}
