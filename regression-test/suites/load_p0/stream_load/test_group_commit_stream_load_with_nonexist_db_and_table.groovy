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

suite("test_group_commit_stream_load_with_nonexist_db_and_table") {
    def tableName = "test_group_commit_stream_load_with_nonexist_db_and_table"

    try {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'group_commit', 'async_mode'
            set 'columns', 'id, name'
            file "test_stream_load1.csv"
            unset 'label'

            time 10000
        }
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
    } finally {

    }
}
