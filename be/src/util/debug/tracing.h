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

#include <deque>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <thread>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "pipeline/pipeline_task.h"
#include "runtime/thread_context.h"

//#define ENABLE_QUERY_DEBUG_TRACE

// The real interfaces for call.
#ifdef ENABLE_QUERY_DEBUG_TRACE

#define TLS_CONTEXT (bthread_context->tls_trace_ctx)

#define SET_THREAD_BASELINE() \
    debug::QueryTrace::set_tls_trace_context(query_trace, fragment_instance_id, task_raw_ptr)

#define SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, task_raw_ptr) \
    debug::QueryTrace::set_tls_trace_context(query_trace, fragment_instance_id, task_raw_ptr)

#define QUERY_TRACE_BEGIN(name, category)        \
    INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER( \
            INTERNAL_CREATE_EVENT_WITH_CTX(name, category, 'B', TLS_CONTEXT))

#define QUERY_TRACE_END(name, category)          \
    INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER( \
            INTERNAL_CREATE_EVENT_WITH_CTX(name, category, 'E', TLS_CONTEXT))

#define QUERY_TRACE_SCOPED(name, category) \
    debug::ScopedTracer _scoped_tracer(name, category)

#define QUERY_TRACE_ASYNC_START(name, category, ctx)                                                            \
    do {                                                                                                        \
        INTERNAL_ADD_EVENT_INFO_BUFFER(ctx.event_buffer,                                                        \
                                       INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(name, category, ctx.id, 'b', ctx)); \
    } while (0);

#define QUERY_TRACE_ASYNC_FINISH(name, category, ctx)                                                           \
    do {                                                                                                        \
        INTERNAL_ADD_EVENT_INFO_BUFFER(ctx.event_buffer,                                                        \
                                       INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(name, category, ctx.id, 'e', ctx)); \
    } while (0);

#else
#define SET_THREAD_BASELINE()
#define SET_THREAD_LOCAL_QUERY_TRACE_CONTEXT(query_trace, fragment_instance_id, driver_ptr)
#define QUERY_TRACE_BEGIN(name, category)
#define QUERY_TRACE_END(name, category)
#define QUERY_TRACE_SCOPED(name, category)
#define QUERY_TRACE_ASYNC_START(name, category, ctx)
#define QUERY_TRACE_ASYNC_FINISH(name, category, ctx)
#endif

#define INTERNAL_CREATE_EVENT_WITH_CTX(name, category, phase, ctx) \
    debug::QueryTraceEvent::create_with_ctx(name, category, ctx.DEFAULT_EVENT_ID, phase, ctx)

#define INTERNAL_CREATE_ASYNC_EVENT_WITH_CTX(name, category, id, phase, ctx) \
    debug::QueryTraceEvent::create_with_ctx(name, category, id, phase, ctx)

#define INTERNAL_ADD_EVENT_INTO_THREAD_LOCAL_BUFFER(event) \
    INTERNAL_ADD_EVENT_INFO_BUFFER(TLS_CONTEXT.event_buffer, event)

#define INTERNAL_ADD_EVENT_INFO_BUFFER(buffer, event) \
    do {                                              \
        if (buffer) {                                 \
            buffer->add(event);                       \
        }                                             \
    } while (0);

// to avoid the problem of not being able to include .h files.
namespace doris::pipeline {
class PipelineTask;
using PipelineTaskRawPtr = PipelineTask*;
}

// The whole function is control by marco in building. if the function is turned off, no entity will be construct.
// In another word, it's zero-overhead.
namespace doris::debug {

class QueryTraceContext;

/// QueryTraceEvent saves a specific event info.
class QueryTraceEvent {
public:
    std::string name;
    std::string category;
    int64_t id; // used for async event
    char phase; // type of event
    int64_t start_time;
    int64_t duration = -1; // used only in compelete event
    decltype(TUniqueId::lo) instance_id;
    pipeline::PipelineTaskRawPtr task; // task pointer
    std::thread::id thread_id;
    std::vector<std::pair<std::string, std::string>> args;

    std::string to_string();

    static QueryTraceEvent create(const std::string& name, const std::string& category, int64_t id, char phase,
                                  int64_t timestamp, int64_t duration, int64_t instance_id, pipeline::PipelineTaskRawPtr task,
                                  std::vector<std::pair<std::string, std::string>>&& args);

    static QueryTraceEvent create_with_ctx(const std::string& name, const std::string& category, int64_t id, char phase,
                                           QueryTraceContext* ctx);

    static QueryTraceEvent create_with_ctx(const std::string& name, const std::string& category, int64_t id, char phase,
                                           int64_t start_ts, int64_t duration, QueryTraceContext* ctx);

private:
    std::string args_to_string();
};

/// Event buffer for a single PipelineTask.
/// didn't store in threads' own trace_context, but in QueryTrace.
class EventBuffer {
public:
    EventBuffer() = default;
    ~EventBuffer() = default;

    void add(QueryTraceEvent&& event);

private:
    friend class QueryTrace;
    std::mutex _mutex;
    std::deque<QueryTraceEvent> _buffer;
};

class QueryTrace {
public:
    QueryTrace(const TUniqueId& query_id, bool is_enable);
    ~QueryTrace() = default;

    // init event buffer for all tasks in a single fragment instance
    void register_tasks(const TUniqueId& fragment_instance_id, pipeline::PipelineTasks& tasks);

    Status dump();

    static void set_tls_trace_context(QueryTrace* query_trace, const TUniqueId& fragment_instance_id,
                                      pipeline::PipelineTaskRawPtr task);

private:
    TUniqueId _query_id;
    [[maybe_unused]] bool _is_enable = false;
    [[maybe_unused]] int64_t _start_ts = -1;

    std::shared_mutex _mutex;
    std::unordered_map<pipeline::PipelineTaskRawPtr, std::unique_ptr<EventBuffer>> _buffers;

    // fragment_instance_id => task list, it will be used to generate meta event
    // instance_id is in PipelineFragmentContext
    std::unordered_map<TUniqueId, std::shared_ptr<std::unordered_set<pipeline::PipelineTaskRawPtr>>> _fragment_tasks;
};

class ScopedTracer {
public:
    ScopedTracer(std::string name, std::string category);
    ~ScopedTracer() noexcept;

private:
    std::string _name;
    std::string _category;
    int64_t _start_ts;
    [[maybe_unused]]int64_t _duration = -1;
};

// the real object is saved in bthread_context, a thread_local ThreadContext object.
// we store/get tls information in it, like buffer slot of this thread to use in query_trace.
class QueryTraceContext {
public:
    static constexpr int64_t DEFAULT_EVENT_ID = 0;

    int64_t start_ts = -1;
    int64_t fragment_instance_id = -1;
    pipeline::PipelineTaskRawPtr task = nullptr;
    int64_t id = DEFAULT_EVENT_ID; // used for async event.
    EventBuffer* event_buffer = nullptr;

    void reset() {
        start_ts = -1;
        fragment_instance_id = -1;
        task = nullptr;
        id = DEFAULT_EVENT_ID;
        event_buffer = nullptr;
    }
};
} // namespace doris::debug
