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

suite("check_before_quit", "nonConcurrent,p0") {
    //NOTE: this suite is used to check whether workload group's query queue works correctly when all query finished
    long beginTime = System.currentTimeMillis();
    long timeoutMs = 300000000 // 300s
    boolean final_check_result = false
    List<List<Object>> result = new ArrayList()
    while ((System.currentTimeMillis() - beginTime) < timeoutMs) {
        result = sql "show workload groups;"
        boolean check_result = true
        for (int i = 0; i < result.size(); i++) {
            List<Object> row = result.get(i)
            int col_size = row.size()
            int running_query_num = Integer.valueOf(row.get(col_size - 2).toString())
            int waiting_query_num = Integer.valueOf(row.get(col_size - 1).toString())
            if (running_query_num != 0 || waiting_query_num != 0) {
                check_result = false
                break
            }
        }
        if (check_result) {
            final_check_result = true
            break
        }
        Thread.sleep(500)
    }

    logger.info("${result}")
    assertTrue(final_check_result)
}
