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
    long timeoutMs = 300 * 1000 // 300s
    boolean clear = false

    while ((System.currentTimeMillis() - beginTime) < timeoutMs) {
        List<List<Object>> result = sql "show workload groups;"
        logger.info("result, ${result}")

        clear = true
        for (int i = 0; i < result.size(); i++) {
            List<Object> row = result.get(i)
            int col_size = row.size()
            int running_query_num = Integer.valueOf(row.get(col_size - 2).toString())
            int waiting_query_num = Integer.valueOf(row.get(col_size - 1).toString())
            if (running_query_num != 0 || waiting_query_num != 0) {
                clear = false
                break
            }
        }

        if (clear) {
            break
        }

        Thread.sleep(500)
    }

    assertTrue(clear)
}
