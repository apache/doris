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

#include <memory>
#include <string>

#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_task.h"
#include "pipeline_task.h"

namespace doris {
class RuntimeState;

namespace pipeline {
class PipelineFragmentContext;

class RevokableTask : public PipelineTask {
public:
    RevokableTask(PipelineTaskSPtr task, std::shared_ptr<SpillContext> spill_context)
            : _task(std::move(task)), _spill_context(std::move(spill_context)) {}

    ~RevokableTask() override = default;

    RuntimeState* runtime_state() const override { return _task->runtime_state(); }

    Status close(Status exec_status, bool close_sink) override {
        return _task->close(exec_status, close_sink);
    }

    Status finalize() override { return _task->finalize(); }

    bool set_running(bool running) override { return _task->set_running(running); }

    bool is_finalized() const override { return _task->is_finalized(); }

    std::weak_ptr<PipelineFragmentContext>& fragment_context() override {
        return _task->fragment_context();
    }

    PipelineTask& set_thread_id(int thread_id) override { return _task->set_thread_id(thread_id); }

    PipelineId pipeline_id() const override { return _task->pipeline_id(); }

    std::string task_name() const override { return _task->task_name(); }

    Status execute(bool* done) override { return _task->do_revoke_memory(_spill_context); }

    bool is_blockable() const override { return true; }

private:
    PipelineTaskSPtr _task;
    std::shared_ptr<SpillContext> _spill_context;
};

} // namespace pipeline
} // namespace doris