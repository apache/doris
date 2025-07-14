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

suite("test_invalid_session") {
    try {
        sql "set parallel_pipeline_task_num = -1;"
    } catch (Exception ex) {
        assert("${ex}".contains("parallel_pipeline_task_num value should greater than or equal 0, you set value is: -1"))
    }

    // test invalid full_sort_max_buffered_bytes
    try {
        sql "set full_sort_max_buffered_bytes = 0;"
    } catch (Exception ex) {
        assert("${ex}".contains("full_sort_max_buffered_bytes value should between"))
    }
    try {
        sql "set full_sort_max_buffered_bytes = -1;"
    } catch (Exception ex) {
        assert("${ex}".contains("full_sort_max_buffered_bytes value should between"))
    }
    try {
        sql "set full_sort_max_buffered_bytes = 16777215;" // test 16MB - 1
    } catch (Exception ex) {
        assert("${ex}".contains("full_sort_max_buffered_bytes value should between"))
    }
    try {
        sql "set full_sort_max_buffered_bytes = 1073741825;" // test 1GB + 1
    } catch (Exception ex) {
        assert("${ex}".contains("full_sort_max_buffered_bytes value should between"))
    }
}
