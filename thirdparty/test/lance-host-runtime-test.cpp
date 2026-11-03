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

#include <arrow/c/abi.h>
#include <lance/lance.h>
#include <unistd.h>

#include <condition_variable>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

// The test owns every worker before entering the SDK. A scan must not add any SDK threads.
class HostPool {
public:
    HostPool() {
        for (int i = 0; i < 4; ++i)
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock lock(mutex);
                        ready.wait(lock, [this] { return stopping || !tasks.empty(); });
                        if (tasks.empty()) return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                }
            });
    }
    ~HostPool() {
        {
            std::lock_guard lock(mutex);
            stopping = true;
        }
        ready.notify_all();
        for (auto& worker : workers) worker.join();
    }
    static void submit(void* context, uint32_t, LanceHostRunTask run, LanceHostRunTask discard,
                       void* task_context) {
        struct Task {
            Task(LanceHostRunTask run, LanceHostRunTask discard, void* context)
                    : run(run), discard(discard), context(context) {}
            LanceHostRunTask run, discard;
            void* context;
            ~Task() {
                if (context) discard(context);
            }
            void execute() {
                auto* owned = std::exchange(context, nullptr);
                run(owned);
            }
        };
        auto task = std::make_shared<Task>(run, discard, task_context);
        auto* pool = static_cast<HostPool*>(context);
        {
            std::lock_guard lock(pool->mutex);
            pool->tasks.emplace([task] { task->execute(); });
        }
        pool->ready.notify_one();
    }

private:
    std::mutex mutex;
    std::condition_variable ready;
    std::queue<std::function<void()>> tasks;
    std::vector<std::thread> workers;
    bool stopping = false;
};

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    HostPool pool;
    LanceHostServicesOptionsV1 services {};
    services.abi_version = 1;
    services.struct_size = sizeof(services);
    services.context = &pool;
    services.submit = HostPool::submit;
    if (lance_runtime_install_host_services_v1(&services) != 0) return 3;
    std::thread reactor([] {
        if (lance_runtime_drive() != 0) std::abort();
    });
    const auto before = std::distance(std::filesystem::directory_iterator("/proc/self/task"),
                                      std::filesystem::directory_iterator());
    LanceHostExecutorOptionsV1 options {};
    options.abi_version = 1;
    options.struct_size = sizeof(options);
    options.context = &pool;
    options.submit_task = HostPool::submit;
    options.max_cpu_drivers = 8;
    options.max_io_drivers = 8;
    auto* executor = lance_host_executor_new_v1(&options);
    auto* scope = lance_host_executor_enter(executor);
    const auto cache_path = std::filesystem::temp_directory_path() /
                            ("lance-host-runtime-" + std::to_string(getpid()));
    std::filesystem::create_directories(cache_path);
    LanceDataCacheOptions cache {};
    const auto cache_directory = cache_path.string();
    cache.directory = cache_directory.c_str();
    cache.memory_capacity_bytes = 8 << 20;
    cache.disk_capacity_bytes = 128 << 20;
    cache.read_block_size_bytes = 1 << 20;
    auto* session = lance_session_new_with_data_cache(16 << 20, 16 << 20, &cache);
    auto* dataset =
            session ? lance_dataset_open_with_session(argv[1], nullptr, 0, session) : nullptr;
    uint64_t rows = 0;
    if (dataset) {
        auto* scanner = lance_scanner_new(dataset, nullptr, nullptr);
        ArrowArrayStream stream {};
        if (scanner && lance_scanner_to_arrow_stream(scanner, &stream) == 0) {
            for (;;) {
                ArrowArray batch {};
                if (stream.get_next(&stream, &batch) != 0 || !batch.release) break;
                rows += static_cast<uint64_t>(batch.length);
                batch.release(&batch);
            }
            stream.release(&stream);
        }
        lance_scanner_close(scanner);
        lance_dataset_close(dataset);
    } else {
        std::cerr << lance_last_error_message() << '\n';
    }
    lance_session_close(session);
    lance_host_executor_leave(scope);
    const auto polls = lance_host_executor_completed_tasks(executor);
    lance_host_executor_free(executor);
    const auto after = std::distance(std::filesystem::directory_iterator("/proc/self/task"),
                                     std::filesystem::directory_iterator());
    lance_runtime_stop();
    reactor.join();
    std::filesystem::remove_all(cache_path);
    std::cout << "rows=" << rows << ", host polls=" << polls << ", threads=" << before << "->"
              << after << '\n';
    return rows > 0 && polls > 0 && before == after ? 0 : 1;
}
