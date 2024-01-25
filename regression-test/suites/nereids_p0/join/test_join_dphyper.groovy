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

suite("test_join_dphyper", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_dphyp_optimizer=true"

    sql "DROP TABLE IF EXISTS test_join_dphyper1"
    sql "DROP TABLE IF EXISTS test_join_dphyper2"
    sql """
            CREATE TABLE test_join_dphyper1(c0 BOOLEAN) DISTRIBUTED BY HASH (c0) PROPERTIES ("replication_num" = "1");
        """
    sql """
            CREATE TABLE test_join_dphyper2(c0 VARCHAR(190) NOT NULL DEFAULT '') DISTRIBUTED BY HASH (c0) BUCKETS 19 PROPERTIES ("replication_num" = "1");
        """

    sql """
            SELECT * FROM test_join_dphyper1, test_join_dphyper2 WHERE ((CASE (test_join_dphyper1.c0 IN ('', test_join_dphyper2.c0, test_join_dphyper1.c0))  WHEN ((test_join_dphyper2.c0) like (test_join_dphyper1.c0)) THEN ((-885441894)|(-1325784872)) ELSE CASE false  WHEN true THEN 1511454646  WHEN true THEN -1171080291  WHEN false THEN 2117897914 END  END )=((- ((1513672096)-(-296074728))))) ORDER BY 1;
    """
    sql "DROP TABLE IF EXISTS test_join_dphyper1"
    sql "DROP TABLE IF EXISTS test_join_dphyper2"
}
