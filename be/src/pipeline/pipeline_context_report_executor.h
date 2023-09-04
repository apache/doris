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
#pragma once

#include <unordered_map>

#include "pipeline/pipeline_fragment_context.h"
#include "pipeline_x/pipeline_x_fragment_context.h"

namespace doris::pipeline {
using WorkFunction = std::function<void()>;

struct ReportTask {
    TUniqueId instance_id;
    WorkFunction work_func;
    std::shared_ptr<PipelineFragmentContext> pipeline_fragment_ctx;
};

class PipelineContextReportExecutor {
public:
    PipelineContextReportExecutor();
    Status submit_report_task(ReportTask&&, bool uniq = false);
    bool is_reporting(TUniqueId instance_id);
    void close();

private:
    std::unique_ptr<ThreadPool> _report_thread_pool;
    std::unordered_map<TUniqueId, std::list<ReportTask>> _report_tasks; // 任务
    std::shared_mutex _report_task_lock;
    bool _closed = false;
};
} // namespace doris::pipeline
