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

#include "pipeline_context_report_executor.h"

namespace doris::pipeline {

PipelineContextReportExecutor::PipelineContextReportExecutor() {
    auto st = ThreadPoolBuilder("FragmentInstanceReportThreadPool")
                      .set_min_threads(48)
                      .set_max_threads(512)
                      .set_max_queue_size(102400)
                      .build(&_report_thread_pool);
    CHECK(st.ok()) << st.to_string();
}

Status PipelineContextReportExecutor::submit_report_task(doris::pipeline::ReportTask&& report_task,
                                                         bool uniq) {
    std::unique_lock<std::shared_mutex> w_lock(_report_task_lock);
    if (_closed) {
        return Status::Aborted("PipelineContextReportExecutor already closed");
    }
    auto instance_id = report_task.instance_id;
    if (uniq && _report_tasks.count(instance_id) > 0) {
        return Status::AlreadyExist("Reporting");
    }
    _report_tasks[instance_id].push_back(report_task);
    auto st = _report_thread_pool->submit_func([this, instance_id, report_task]() {
        report_task.work_func();
        std::unique_lock<std::shared_mutex> w_lock(_report_task_lock);
        _report_tasks[report_task.instance_id].pop_front();
        if (_report_tasks[report_task.instance_id].empty()) {
            _report_tasks.erase(report_task.instance_id);
        }
    });
    if (!st.ok()) {
        _report_tasks[report_task.instance_id].pop_back();
        if (_report_tasks[report_task.instance_id].empty()) {
            _report_tasks.erase(report_task.instance_id);
        }
        return st;
    } else {
        return Status::OK();
    }
}

bool PipelineContextReportExecutor::is_reporting(TUniqueId instance_id) {
    std::shared_lock<std::shared_mutex> r_lock(_report_task_lock);
    return _report_tasks.count(instance_id) > 0;
}

void PipelineContextReportExecutor::close() {
    {
        std::unique_lock<std::shared_mutex> w_lock(_report_task_lock);
        if (_closed) {
            return;
        } else {
            _closed = true;
        }
    }
    _report_thread_pool->shutdown();
}

} // namespace doris::pipeline
