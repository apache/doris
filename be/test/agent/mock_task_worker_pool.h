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

#ifndef DORIS_BE_SRC_MOCK_MOCK_TASK_WORKER_POOL_H
#define DORIS_BE_SRC_MOCK_MOCK_TASK_WORKER_POOL_H

#include "agent/task_worker_pool.h"
#include "common/status.h"

namespace doris {

const uint32_t TASK_FINISH_MAX_RETRY = 3;
const uint32_t PUSH_MAX_RETRY = 1;
const uint32_t REPORT_TASK_WORKER_COUNT = 1;
const uint32_t REPORT_DISK_STATE_WORKER_COUNT = 1;
const uint32_t REPORT_OLAP_TABLE_WORKER_COUNT = 1;
const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;

class MockTaskWorkerPool : public TaskWorkerPool {
public:
    MOCK_METHOD0(start, void());
    MOCK_METHOD1(submit_task, void(const TAgentTaskRequest& task));
    MOCK_METHOD0(get_command_executor, CommandExecutor*());
}; // class MockTaskWorkerPool
} // namespace doris
#endif // DORIS_BE_SRC_MOCK_MOCK_TASK_WORKER_POOL_H
