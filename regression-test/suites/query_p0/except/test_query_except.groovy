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

suite("test_query_except", "arrow_flight_sql") {
    // test query except, depend on query_test_data_load.groovy
    sql "use test_query_db"
    qt_select_except1 """
                      SELECT * FROM (SELECT k1 FROM test_query_db.baseall
                                     EXCEPT SELECT k1 FROM test_query_db.test) a ORDER BY k1
                      """
    qt_select_except2 """
		      select not_null_k1, not_null_k1 from (SELECT non_nullable(k1) as not_null_k1 FROM test_query_db.baseall where k1 is not null) b1 except select non_nullable(k1), k1 from test_query_db.baseall where k1 is not null order by 1, 2;	
		      """
}
