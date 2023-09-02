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

suite("test_scan_topn_limit") {
    def tableName = "test_scan_topn_limit"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k1 int,
                k2 int
            )
            DUPLICATE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """

    for (i in range(1, 30)) {
        sql """ INSERT INTO ${tableName} values (${i}, 100-${i}) """
    }
    qt_select """ SELECT /*+ SET_VAR(experimental_enable_pipeline_engine = true,
                            parallel_fragment_exec_instance_num = 4) */ 
                        k1, k2 FROM ${tableName} ORDER BY k1 DESC, k2 LIMIT 10 """
}
