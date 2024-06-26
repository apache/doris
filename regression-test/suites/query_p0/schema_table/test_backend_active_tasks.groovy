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

suite("test_backend_active_tasks") {
    def thread1 = new Thread({
        for (int i = 0; i <= 300; i++) {
            // non-pipeline
            sql "set experimental_enable_pipeline_engine=false"
            sql "set experimental_enable_pipeline_x_engine=false"
            sql "select * from information_schema.backend_active_tasks"
            sql "select BE_ID,FE_HOST,QUERY_ID,SCAN_ROWS from information_schema.backend_active_tasks"

            // pipeline
            sql "set experimental_enable_pipeline_engine=true"
            sql "set experimental_enable_pipeline_x_engine=false"
            sql "select * from information_schema.backend_active_tasks"
            sql "select BE_ID,FE_HOST,QUERY_ID,SCAN_ROWS from information_schema.backend_active_tasks"

            // pipelinex
            sql "set experimental_enable_pipeline_engine=true"
            sql "set experimental_enable_pipeline_x_engine=true"
            sql "select * from information_schema.backend_active_tasks"
            sql "select BE_ID,FE_HOST,QUERY_ID,SCAN_ROWS from information_schema.backend_active_tasks"
            Thread.sleep(1000)
        }
    })
    thread1.setDaemon(true)
    thread1.start()
}
