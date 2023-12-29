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

#include "dependency.h"

#include <memory>
#include <mutex>

#include "common/logging.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/pipeline_x/local_exchange/local_exchanger.h"
#include "pipeline/pipeline_x/pipeline_x_task.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"

namespace doris::pipeline {

void Dependency::_add_block_task(PipelineXTask* task) {
    DCHECK(_blocked_task.empty() || _blocked_task[_blocked_task.size() - 1] != task)
            << "Duplicate task: " << task->debug_string();
    _blocked_task.push_back(task);
}

void Dependency::set_ready() {
    if (_ready) {
        return;
    }
    _watcher.stop();
    std::vector<PipelineXTask*> local_block_task {};
    {
        std::unique_lock<std::mutex> lc(_task_lock);
        if (_ready) {
            return;
        }
        _ready = true;
        local_block_task.swap(_blocked_task);
    }
    for (auto* task : local_block_task) {
        task->wake_up();
    }
}

Dependency* Dependency::is_blocked_by(PipelineXTask* task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    auto ready = _ready.load() || _is_cancelled();
    if (!ready && task) {
        _add_block_task(task);
    }
    return ready ? nullptr : this;
}

Dependency* FinishDependency::is_blocked_by(PipelineXTask* task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    auto ready = _ready.load();
    if (!ready && task) {
        _add_block_task(task);
    }
    return ready ? nullptr : this;
}

Dependency* RuntimeFilterDependency::is_blocked_by(PipelineXTask* task) {
    if (!_blocked_by_rf) {
        return nullptr;
    }
    std::unique_lock<std::mutex> lc(_task_lock);
    if (*_blocked_by_rf && !_is_cancelled()) {
        if (LIKELY(task)) {
            _add_block_task(task);
        }
        return this;
    }
    return nullptr;
}

std::string Dependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, block task = {}, ready={}",
                   std::string(indentation_level * 2, ' '), _name, _node_id, _blocked_task.size(),
                   _ready);
    return fmt::to_string(debug_string_buffer);
}

std::string RuntimeFilterDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}{}: id={}, block task = {}, ready={}, _filters = {}, _blocked_by_rf = {}",
                   std::string(indentation_level * 2, ' '), _name, _node_id, _blocked_task.size(),
                   _ready, _filters.load(), _blocked_by_rf ? _blocked_by_rf->load() : false);
    return fmt::to_string(debug_string_buffer);
}

std::string AndDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, children=[",
                   std::string(indentation_level * 2, ' '), _name, _node_id);
    for (auto& child : _children) {
        fmt::format_to(debug_string_buffer, "{}, \n", child->debug_string(indentation_level = 1));
    }
    fmt::format_to(debug_string_buffer, "{}]", std::string(indentation_level * 2, ' '));
    return fmt::to_string(debug_string_buffer);
}

bool RuntimeFilterTimer::has_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    return _is_ready;
}

void RuntimeFilterTimer::call_timeout() {
    std::unique_lock<std::mutex> lc(_lock);
    if (_call_ready) {
        return;
    }
    _call_timeout = true;
    if (_parent) {
        _parent->sub_filters();
    }
}

void RuntimeFilterTimer::call_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    if (_call_timeout) {
        return;
    }
    _call_ready = true;
    if (_parent) {
        _parent->sub_filters();
    }
    _is_ready = true;
}

void RuntimeFilterTimer::call_has_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    DCHECK(!_call_timeout);
    if (!_call_ready) {
        _parent->sub_filters();
    }
}

void RuntimeFilterTimer::call_has_release() {
    // When the use count is equal to 1, only the timer queue still holds ownership,
    // so there is no need to take any action.
}

void RuntimeFilterDependency::add_filters(IRuntimeFilter* runtime_filter) {
    _filters++;
    int64_t registration_time = runtime_filter->registration_time();
    int32 wait_time_ms = runtime_filter->wait_time_ms();
    auto filter_timer = std::make_shared<RuntimeFilterTimer>(
            registration_time, wait_time_ms,
            std::dynamic_pointer_cast<RuntimeFilterDependency>(shared_from_this()));
    runtime_filter->set_filter_timer(filter_timer);
    ExecEnv::GetInstance()->runtime_filter_timer_queue()->push_filter_timer(filter_timer);
}

void RuntimeFilterDependency::sub_filters() {
    auto value = _filters.fetch_sub(1);
    if (value == 1) {
        _watcher.stop();
        std::vector<PipelineXTask*> local_block_task {};
        {
            std::unique_lock<std::mutex> lc(_task_lock);
            *_blocked_by_rf = false;
            local_block_task.swap(_blocked_task);
        }
        for (auto* task : local_block_task) {
            task->wake_up();
        }
    }
}

void LocalExchangeSharedState::sub_running_sink_operators() {
    std::unique_lock<std::mutex> lc(le_lock);
    if (exchanger->_running_sink_operators.fetch_sub(1) == 1) {
        _set_ready_for_read();
    }
}

} // namespace doris::pipeline
