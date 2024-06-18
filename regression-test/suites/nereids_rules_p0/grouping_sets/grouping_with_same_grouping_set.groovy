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
suite("grouping_with_same_grouping_set") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
         DROP TABLE IF EXISTS t1;
        """
    sql """
        create table t1(a int) distributed by hash(a) buckets 1 properties ( 'replication_num' = '1');
    """
    sql """
        insert into t1 values (1), (2), (3);
    """
    qt_test "select max(a) from t1 group by grouping sets ((a), (a)) order by 1"

}
