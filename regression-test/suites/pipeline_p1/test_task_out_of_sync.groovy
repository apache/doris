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

suite("test_task_out_of_sync", "nonConcurrent") {
    sql "SET enable_profile = true" // need profile to know where the wrong is
    sql "set debug_skip_fold_constant = true" // test BE

    try {
        GetDebugPoint().enableDebugPointForAllBEs("VectorizedFnCall.wait_before_execute", [possibility: 0.2])
        for (int i = 0; i < 30; i++) {
            try {
                def result = sql "select cast(timediff('2020-05-04 17:00:00', '2020-05-05 00:00:00') as int)"
                assertEquals(result.size(), 1) // only one row
            } catch (org.opentest4j.AssertionFailedError e) {
                // wrong rows quantity!
                logger.info("AssertionFailedError: ${e}")
                assert false
            } catch (Exception e) {
                // sql failed is expected
            }
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("VectorizedFnCall.wait_before_execute")
    }
}
