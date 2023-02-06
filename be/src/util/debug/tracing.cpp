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

#include <gen_cpp/Opcodes_types.h>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <utility>

#include "common/config.h"
#include "pipeline/pipeline_task.h"
#include "fmt/printf.h"
#include "util/debug/tracing.h"
#include "util/time.h"

namespace doris::debug {

QueryTraceEvent QueryTraceEvent::create(const std::string& name, const std::string& category, int64_t id, char phase,
                                        int64_t timestamp, int64_t duration, int64_t instance_id, pipeline::PipelineTaskRawPtr task,
                                        std::vector<std::pair<std::string, std::string>>&& args) {
    QueryTraceEvent event;
    event.name = name;
    event.category = category;
    event.id = id;
    event.phase = phase;
    event.start_time = timestamp;
    event.duration = duration;
    event.instance_id = instance_id;
    event.task = task;
    event.args = std::move(args);
    event.thread_id = std::this_thread::get_id();
    return event;
}

QueryTraceEvent QueryTraceEvent::create_with_ctx(const std::string& name, const std::string& category, int64_t id,
                                                 char phase, QueryTraceContext* ctx) {
    return create(name, category, id, phase, MonotonicMicros() - ctx->start_ts, QueryTraceContext::DEFAULT_EVENT_ID,
                  ctx->fragment_instance_id, ctx->task, {});
}

QueryTraceEvent QueryTraceEvent::create_with_ctx(const std::string& name, const std::string& category, int64_t id,
                                                 char phase, int64_t start_ts, int64_t duration,
                                                 QueryTraceContext* ctx) {
    return create(name, category, id, phase, start_ts, duration, ctx->fragment_instance_id, ctx->task, {});
}

static const char* kSimpleEventFormat =
        R"({"cat":"%s","name":"%s","pid":"0x%x","tid":"0x%x","id":"0x%x","ts":%ld,"ph":"%c","args":%s,"tidx":"0x%x"})";
static const char* kCompleteEventFormat =
        R"({"cat":"%s","name":"%s","pid":"0x%x","tid":"0x%x","id":"0x%x","ts":%ld,"dur":%ld,"ph":"%c","args":%s,"tidx":"0x%x"})";

std::string QueryTraceEvent::to_string() {
    std::string args_str = args_to_string();
    size_t tidx = std::hash<std::thread::id>{}(thread_id);

    // transform a raw pointer to uint64_t by re_cast is maybe not the best way, but at least unambiguous & safe.
    if (phase == 'X') {
        return fmt::sprintf(kCompleteEventFormat, category.c_str(), name.c_str(), (uint64_t)instance_id,
                            reinterpret_cast<uint64_t>(task), id, start_time, duration, phase, args_str.c_str(), tidx);
    } else {
        return fmt::sprintf(kSimpleEventFormat, category.c_str(), name.c_str(), (uint64_t)instance_id,
                            reinterpret_cast<uint64_t>(task), id, start_time, phase, args_str.c_str(), tidx);
    }
}

std::string QueryTraceEvent::args_to_string() {
    if (args.empty()) {
        return "{}";
    }
    std::ostringstream oss;
    oss << "{";
    oss << fmt::sprintf(R"("%s":"%s")", args[0].first.c_str(), args[0].second.c_str());
    for (size_t i = 1; i < args.size(); i++) {
        oss << fmt::sprintf(R"(,"%s":"%s")", args[i].first.c_str(), args[i].second.c_str());
    }
    oss << "}";
    return oss.str();
}

void EventBuffer::add(QueryTraceEvent&& event) {
    std::lock_guard<std::mutex> l(_mutex);
    _buffer.emplace_back(std::move(event));
}

QueryTrace::QueryTrace(const TUniqueId& query_id, bool is_enable) : _query_id(query_id), _is_enable(is_enable) {
    if (_is_enable) {
        _start_ts = MonotonicMicros();
    }
}

void QueryTrace::register_tasks(const TUniqueId& fragment_instance_id, pipeline::PipelineTasks& tasks) {
    if (!_is_enable) {
        return;
    }
    std::unique_lock l(_mutex);
    auto iter = _fragment_tasks.find(fragment_instance_id);
    if (iter == _fragment_tasks.end()) {
        _fragment_tasks.emplace(fragment_instance_id,
                                    std::make_shared<std::unordered_set<pipeline::PipelineTaskRawPtr>>());
        iter = _fragment_tasks.find(fragment_instance_id);
    }
    for (auto& task : tasks) {
        iter->second->insert(task); // into this fragment's task map.
        _buffers.emplace(task, std::make_unique<EventBuffer>());
    }
}

Status QueryTrace::dump() {
    if (!_is_enable) {
        return Status::OK();
    }
    static const char* kProcessNameMetaEventFormat =
            "{\"name\":\"process_name\",\"ph\":\"M\",\"pid\":\"0x%x\",\"args\":{\"name\":\"%s\"}}";
    static const char* kThreadNameMetaEventFormat =
            "{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":\"0x%x\",\"tid\":\"0x%x\",\"args\":{\"name\":\"%s\"}}";
    try {
        std::filesystem::create_directory(doris::config::tracing_dir);
        std::string file_name =
                fmt::format("{}/{}.json", doris::config::tracing_dir, print_id(_query_id));
        std::ofstream oss(file_name.c_str(), std::ios::out | std::ios::binary);
        oss << "{\"traceEvents\":[\n";
        bool is_first = true;
        for (auto& [fragment_id, task_set] : _fragment_tasks) {
            // task_set's type is shared_ptr<umap<PipelineTask*>>
            std::string fragment_id_str = print_id(fragment_id);
            oss << (is_first ? "" : ",\n");
            oss << fmt::sprintf(kProcessNameMetaEventFormat, (uint64_t)fragment_id.lo, fragment_id_str.c_str());
            is_first = false;
            for (auto& task : *task_set) {
                pipeline::PipelineTaskRawPtr ptr = task;
                oss << (is_first ? "" : ",\n");
                oss << fmt::sprintf(kThreadNameMetaEventFormat, (uint64_t)fragment_id.lo, reinterpret_cast<uint64_t>(task),
                                    std::to_string(ptr->index()));
            }
        }

        for (auto& [_, buffer_ptr] : _buffers) {
            auto& buffer = buffer_ptr->_buffer;
            for (auto iter : buffer) {
                oss << (is_first ? "" : ",\n");
                oss << iter.to_string();
            }
        }
        oss << "\n]}";

        oss.close();
    } catch (std::exception& e) {
        return Status::IOError(fmt::format("dump trace log error {}", e.what()));
    }
    return Status::OK();
}

void QueryTrace::set_tls_trace_context(QueryTrace* query_trace, const TUniqueId& fragment_instance_id,
                                       pipeline::PipelineTaskRawPtr task) {
#ifdef ENABLE_QUERY_DEBUG_TRACE
    if (!query_trace->_is_enable) {
        TLS_CONTEXT->reset();
        return;
    }
    {
        std::shared_lock l(query_trace->_mutex);
        auto iter = query_trace->_buffers.find(task);
        DCHECK(iter != query_trace->_buffers.end());
        TLS_CONTEXT->event_buffer = iter->second.get();
    }
    TLS_CONTEXT->start_ts = query_trace->_start_ts;
    TLS_CONTEXT->fragment_instance_id = fragment_instance_id.lo;
    TLS_CONTEXT->task = task;
#endif
}

ScopedTracer::ScopedTracer(std::string name, std::string category)
        : _name(std::move(name)), _category(std::move(category)) {
    _start_ts = MonotonicMicros();
}

ScopedTracer::~ScopedTracer() noexcept {
#ifdef ENABLE_QUERY_DEBUG_TRACE
    if (TLS_CONTEXT->event_buffer != nullptr) {
        _duration = MonotonicMicros() - _start_ts;
        TLS_CONTEXT->event_buffer->add(
                QueryTraceEvent::create_with_ctx(_name, _category, QueryTraceContext::DEFAULT_EVENT_ID, 'X',
                                                 _start_ts - TLS_CONTEXT->start_ts, _duration, TLS_CONTEXT));
    }
#endif
}
} // namespace debug
