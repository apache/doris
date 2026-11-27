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

#include "format_v2/lance/lance_host_runtime.h"

#include <lance/lance.h>

#include <atomic>
#include <utility>

#include "common/config.h"
#include "common/metrics/doris_metrics.h"
#include "exec/scan/scanner_scheduler.h"
#include "format_v2/lance/lance_memory.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/threadpool.h"

namespace doris::format::lance {
struct LanceHostRuntime::Task {
    Task(void (*run)(void*), void (*discard)(void*), void* context)
            : run(run), discard(discard), context(context) {}
    ~Task() {
        if (context) discard(context);
    }
    void execute() {
        auto* owned = std::exchange(context, nullptr);
        if (owned) run(owned);
    }
    void (*run)(void*);
    void (*discard)(void*);
    void* context;
};
struct LanceHostRuntime::QueryExecutor {
    std::atomic<uint32_t> refs {1};
    std::weak_ptr<LanceHostRuntime> runtime;
    std::weak_ptr<QueryContext> query;
    std::atomic<uint64_t> sequence {0};
};

std::shared_ptr<LanceHostRuntime> LanceHostRuntime::instance() {
    // These services must outlive our pool metrics and shared I/O token at process shutdown.
    static_cast<void>(DorisMetrics::instance());
    static_cast<void>(ExecEnv::GetInstance());
    static auto runtime = std::make_shared<LanceHostRuntime>();
    return runtime;
}
Status LanceHostRuntime::ensure_started() {
    if (_stopped) return Status::InternalError("Lance host runtime has stopped");
    std::call_once(_once, [this] { _status = start(); });
    return _status;
}
Status LanceHostRuntime::start() {
    RETURN_IF_ERROR(ThreadPoolBuilder("LanceReactor")
                            .set_min_threads(0)
                            .set_max_threads(1)
                            .build(&_reactor));
    RETURN_IF_ERROR(ThreadPoolBuilder("LanceBackground")
                            .set_min_threads(0)
                            .set_max_threads(config::lance_background_thread_pool_size)
                            .build(&_background));
    auto* io_pool = ExecEnv::GetInstance()->s3_file_system_thread_pool();
    if (io_pool == nullptr) {
        // Focused reader tests can run before ExecEnv initializes the shared I/O pool.
        RETURN_IF_ERROR(ThreadPoolBuilder("LanceIO")
                                .set_min_threads(0)
                                .set_max_threads(std::max(1, config::max_s3_file_system_thread_num))
                                .build(&_io));
        io_pool = _io.get();
    }
    _io_token = io_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    LanceHostServicesOptionsV1 options {};
    options.abi_version = 1;
    options.struct_size = sizeof(options);
    options.context = this;
    options.submit = submit_service;
    RETURN_IF_ERROR(install_lance_memory_services());
    if (lance_runtime_install_host_services_v1(&options) != 0)
        return lance_error("install host runtime services");
    RETURN_IF_ERROR(_reactor->submit_func([] {
        SCOPED_INIT_THREAD_CONTEXT();
        const auto status = lance_runtime_drive();
        if (status != 0) LOG(ERROR) << "Lance host reactor failed";
    }));
    // The stop fence needs a live driver; failed submission must not wait for one at teardown.
    _installed = true;
    return Status::OK();
}
LanceHostRuntime::~LanceHostRuntime() {
    shutdown();
}
void LanceHostRuntime::shutdown() {
    {
        std::lock_guard lock(_dispatch_mutex);
        if (_stopped.exchange(true)) return;
    }
    // Fragment shutdown and session close precede this call in ExecEnv::destroy.
    // Drain workers while the reactor still handles joins and cancellation notifications.
    if (_io_token) _io_token->shutdown();
    _io_token.reset();
    if (_io) _io->shutdown();
    // Cleanup jobs remain admissible while rejected work and SDK wakeups drain.
    // Stopping this pool first would either strand destructors or run them on the reactor.
    if (_background) _background->wait();
    if (_installed) lance_runtime_stop();
    if (_reactor) _reactor->shutdown();
    if (_background) _background->shutdown();
    _reactor.reset();
    _background.reset();
    _io.reset();
}
void LanceHostRuntime::submit(uint32_t queue, std::shared_ptr<Task> task) {
    std::lock_guard lock(_dispatch_mutex);
    if (_stopped && queue != 2) return;
    // Dropping a rejected callback cancels its Rust task; it must not escape or run inline
    // while Rust's wake callback could be holding an SDK lock.
    auto work = [task = std::move(task)] {
        SCOPED_INIT_THREAD_CONTEXT();
        task->execute();
    };
    auto status = queue == 1 ? _io_token->submit_func(std::move(work))
                             : _background->submit_func(std::move(work));
    if (!status.ok()) {
        LOG_EVERY_N(WARNING, 100) << "Lance host task rejected: " << status;
    }
}
void LanceHostRuntime::submit_service(void* context, uint32_t queue, void (*run)(void*),
                                      void (*discard)(void*), void* task_context) {
    try {
        auto task = std::make_shared<Task>(run, discard, task_context);
        // Ownership has moved: an enqueue exception must not discard the native task twice.
        task_context = nullptr;
        static_cast<LanceHostRuntime*>(context)->submit(queue, std::move(task));
    } catch (...) {
        if (task_context) discard(task_context);
    }
}
void LanceHostRuntime::retain_query(void* context) {
    static_cast<QueryExecutor*>(context)->refs.fetch_add(1, std::memory_order_relaxed);
}
void LanceHostRuntime::release_query(void* context) {
    auto* executor = static_cast<QueryExecutor*>(context);
    if (executor->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) delete executor;
}
void LanceHostRuntime::submit_query(void* context, uint32_t queue, void (*run)(void*),
                                    void (*discard)(void*), void* task_context) {
    try {
        auto* executor = static_cast<QueryExecutor*>(context);
        auto task = std::make_shared<Task>(run, discard, task_context);
        // Ownership has moved: an enqueue exception must not discard the native task twice.
        task_context = nullptr;
        auto owner = executor->runtime.lock();
        if (!owner) return;
        if (queue == 2) {
            owner->submit(queue, std::move(task));
            return;
        }
        if (auto query = executor->query.lock()) {
            std::unique_lock dispatch(owner->_dispatch_mutex);
            if (owner->_stopped) return;
            auto work = [task, query] {
                SCOPED_ATTACH_TASK(query.get());
                task->execute();
                return true;
            };
            if (queue == 0 && query->get_scan_scheduler()) {
                // Use the query's existing scheduler, including its workload-group CPU limits.
                // The synchronous caller also helps its children, so pool saturation cannot deadlock.
                const auto id = fmt::format("lance-{}-{}", reinterpret_cast<uintptr_t>(executor),
                                            executor->sequence.fetch_add(1));
                auto status = query->get_scan_scheduler()->submit_scan_task(
                        SimplifiedScanTask(std::move(work), nullptr, nullptr), id);
                if (!status.ok()) {
                    LOG_EVERY_N(WARNING, 100) << "Lance scan task rejected: " << status;
                }
            } else {
                auto execute = [work = std::move(work)] { work(); };
                auto status = queue == 1 ? owner->_io_token->submit_func(std::move(execute))
                                         : owner->_background->submit_func(std::move(execute));
                if (!status.ok()) {
                    LOG_EVERY_N(WARNING, 100) << "Lance I/O task rejected: " << status;
                }
            }
        } else {
            owner->submit(queue, std::move(task));
        }
    } catch (...) {
        if (task_context) discard(task_context);
    }
}
Status LanceHostRuntime::create_executor(RuntimeState* state, ::LanceHostExecutor** out) {
    RETURN_IF_ERROR(ensure_started());
    auto* context = new QueryExecutor;
    context->runtime = shared_from_this();
    if (state && state->get_query_ctx()) context->query = state->get_query_ctx_weak();
    LanceHostExecutorOptionsV1 options {};
    options.abi_version = 1;
    options.struct_size = sizeof(options);
    options.context = context;
    options.retain_context = retain_query;
    options.release_context = release_query;
    options.submit_task = submit_query;
    options.max_cpu_drivers = config::lance_cpu_parallelism;
    options.max_io_drivers = std::min(config::lance_io_parallelism,
                                      std::max(1, config::max_s3_file_system_thread_num));
    *out = lance_host_executor_new_v1(&options);
    release_query(context);
    if (*out == nullptr) return lance_error("create query host executor");
    auto status = attach_lance_memory_account(*out, state);
    if (!status.ok()) {
        lance_host_executor_free(*out);
        *out = nullptr;
    }
    return status;
}
} // namespace doris::format::lance
