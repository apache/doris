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

suite("test_outerjoin_isnull_estimation") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS test_outerjoin_isnull_estimation1"
    sql """ CREATE TABLE `test_outerjoin_isnull_estimation1` (
	    c1 int, c2 int, c3 int
    )ENGINE=OLAP
    distributed by hash(c1) buckets 10
    properties(
        "replication_allocation" = "tag.location.default: 1"
    );"""

    sql "DROP TABLE IF EXISTS test_outerjoin_isnull_estimation2"
    sql """ CREATE TABLE `test_outerjoin_isnull_estimation2` (
	    c1 int, c2 int, c3 int
    )ENGINE=OLAP
    distributed by hash(c1) buckets 10
    properties(
        "replication_allocation" = "tag.location.default: 1"
    );"""

    sql "insert into test_outerjoin_isnull_estimation1 values (1,1,1);"
    sql "insert into test_outerjoin_isnull_estimation1 values (2,2,2);"
    sql "insert into test_outerjoin_isnull_estimation2 values (3,3,3);"
    sql "insert into test_outerjoin_isnull_estimation2 values (4,4,4);"

    sql "analyze table test_outerjoin_isnull_estimation1 with full with sync;"
    sql "analyze table test_outerjoin_isnull_estimation2 with full with sync;"

    explain {
        sql("physical plan select t1.c1, t1.c2 from test_outerjoin_isnull_estimation1 t1" +
                " left join test_outerjoin_isnull_estimation1 t2 on t1.c1 = t2.c1 where t2.c2 is null;");
        contains"stats=1, predicates=c2#4 IS NULL"
        notContains"stats=0"
    }
}
