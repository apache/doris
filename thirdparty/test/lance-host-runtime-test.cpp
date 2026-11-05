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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// The C++ callback ABI must balance Rust-owned buffers after all SDK handles close.
struct MemoryLedger {
    std::atomic<int64_t> bytes {0};
    std::atomic<int64_t> peak {0};
    std::atomic<int64_t> references {1};
    std::atomic<uint64_t> reservations {0};
    static void record(void* context, int64_t delta) noexcept {
        auto* ledger = static_cast<MemoryLedger*>(context);
        const auto now = ledger->bytes.fetch_add(delta) + delta;
        auto previous = ledger->peak.load();
        while (previous < now && !ledger->peak.compare_exchange_weak(previous, now)) {
        }
    }
    LanceHostMemoryAccountOptionsV1 options() {
        LanceHostMemoryAccountOptionsV1 result {};
        result.abi_version = 1;
        result.struct_size = sizeof(result);
        result.context = this;
        result.retain_context = [](void* p) { static_cast<MemoryLedger*>(p)->references++; };
        result.release_context = [](void* p) { static_cast<MemoryLedger*>(p)->references--; };
        result.record = record;
        result.reserve = [](void* p, size_t size) -> int32_t {
            if (size > static_cast<size_t>(std::numeric_limits<int64_t>::max())) return 1;
            auto* ledger = static_cast<MemoryLedger*>(p);
            ledger->reservations++;
            record(p, static_cast<int64_t>(size));
            return 0;
        };
        result.finish_reserve = [](void* p, size_t size, bool allocated) {
            if (!allocated) record(p, -static_cast<int64_t>(size));
        };
        result.check = [](void*) -> int32_t { return 0; };
        return result;
    }
};

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

struct HostStorage {
    std::atomic<uint64_t> reads {0};
    std::atomic<uint64_t> files {0};
    std::atomic<uint64_t> refs {1};
    struct File {
        HostStorage* owner;
        std::ifstream input;
    };
    static std::string path(const char* uri) {
        std::string value(uri);
        return value.starts_with("file://") ? value.substr(7) : value;
    }
    static int32_t open(void* context, const char* uri, void** out, LanceHostFileMeta* meta,
                        const char**) {
        auto* storage = static_cast<HostStorage*>(context);
        const auto name = path(uri);
        if (!std::filesystem::exists(name)) return 1;
        auto file = std::make_unique<File>();
        file->owner = storage;
        file->input.open(name, std::ios::binary);
        if (!file->input) return 2;
        meta->size = std::filesystem::file_size(name);
        meta->modified_seconds = 0;
        storage->files++;
        *out = file.release();
        return 0;
    }
    static int32_t read(void* context, uint64_t offset, uint8_t* buffer, size_t length,
                        size_t* count, const char**) {
        auto* file = static_cast<File*>(context);
        file->input.clear();
        file->input.seekg(offset);
        file->input.read(reinterpret_cast<char*>(buffer), length);
        *count = file->input.gcount();
        file->owner->reads++;
        return file->input.bad() ? 2 : 0;
    }
    static void close(void* context) {
        std::unique_ptr<File> file(static_cast<File*>(context));
        file->owner->files--;
    }
    static int32_t list(void*, const char* uri, LanceHostListEntry emit, void* context,
                        const char**) {
        const auto root = path(uri);
        if (!std::filesystem::exists(root)) return 0;
        for (const auto& file : std::filesystem::recursive_directory_iterator(root)) {
            if (!file.is_regular_file()) continue;
            const auto name = "file://" + file.path().string();
            LanceHostFileMeta meta {file.file_size(), 0};
            if (emit(context, name.c_str(), &meta) != 0) return 2;
        }
        return 0;
    }
};

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    HostPool pool;
    LanceHostServicesOptionsV1 services {};
    services.abi_version = 1;
    services.struct_size = sizeof(services);
    services.context = &pool;
    services.submit = HostPool::submit;
    // Service contexts have process lifetime, independently of the query ledger below.
    static MemoryLedger background_memory, index_memory, metadata_memory;
    LanceHostMemoryServicesOptionsV1 memory_services {};
    memory_services.abi_version = 1;
    memory_services.struct_size = sizeof(memory_services);
    memory_services.background = background_memory.options();
    memory_services.index_cache = index_memory.options();
    memory_services.metadata_cache = metadata_memory.options();
    if (lance_runtime_install_memory_services_v1(&memory_services) != 0) return 3;
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
    MemoryLedger query_memory;
    auto query_memory_options = query_memory.options();
    if (!executor || lance_host_executor_set_memory_v1(executor, &query_memory_options) != 0)
        return 3;
    auto* scope = lance_host_executor_enter(executor);
    HostStorage storage;
    LanceHostStorageOptionsV1 storage_options {};
    storage_options.abi_version = 1;
    storage_options.struct_size = sizeof(storage_options);
    storage_options.context = &storage;
    storage_options.retain_context = [](void* p) { static_cast<HostStorage*>(p)->refs++; };
    storage_options.release_context = [](void* p) { static_cast<HostStorage*>(p)->refs--; };
    storage_options.cache_namespace = "native-host-fixture";
    storage_options.open = HostStorage::open;
    storage_options.read_at = HostStorage::read;
    storage_options.close = HostStorage::close;
    storage_options.list = HostStorage::list;
    auto* session = lance_session_new(16 << 20, 16 << 20);
    auto* dataset = session ? lance_dataset_open_with_storage_v1(argv[1], nullptr, 0, session,
                                                                 &storage_options)
                            : nullptr;
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
        struct Completion {
            std::atomic<bool> done {false};
            int32_t status = -1;
            void* result = nullptr;
        } completion;
        scanner = lance_scanner_new(dataset, nullptr, nullptr);
        lance_scanner_scan_async(
                scanner,
                [](void* context, int32_t status, void* result) {
                    auto* completion = static_cast<Completion*>(context);
                    completion->status = status;
                    completion->result = result;
                    completion->done.store(true, std::memory_order_release);
                },
                &completion);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!completion.done.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= deadline) std::abort();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (completion.status != 0) std::abort();
        lance_scanner_async_stream_free(static_cast<ArrowArrayStream*>(completion.result));
        lance_scanner_close(scanner);
        lance_dataset_close(dataset);
    } else {
        std::cerr << lance_last_error_message() << '\n';
    }
    lance_session_close(session);
    lance_host_executor_leave(scope);
    const auto polls = lance_host_executor_completed_tasks(executor);
    lance_host_executor_free(executor);
    // Closing an unconsumed async stream cancels host pumps asynchronously. Keep services
    // running until those jobs release their retained storage context.
    const auto cleanup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((storage.refs != 1 || storage.files != 0 || query_memory.bytes != 0 ||
            query_memory.references != 1) &&
           std::chrono::steady_clock::now() < cleanup_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto after = std::distance(std::filesystem::directory_iterator("/proc/self/task"),
                                     std::filesystem::directory_iterator());
    lance_runtime_stop();
    reactor.join();

    const auto reservations = query_memory.reservations.load() +
                              metadata_memory.reservations.load() +
                              index_memory.reservations.load();
    std::cout << "rows=" << rows << ", host polls=" << polls << ", threads=" << before << "->"
              << after << ", host reads=" << storage.reads << ", open files=" << storage.files
              << ", refs=" << storage.refs << ", query memory=" << query_memory.bytes
              << ", query peak=" << query_memory.peak << ", reservations=" << reservations << '\n';
    return rows > 0 && polls > 0 && before == after && storage.reads > 0 && storage.files == 0 &&
                           storage.refs == 1 && query_memory.bytes == 0 &&
                           query_memory.references == 1 && query_memory.peak > 0 && reservations > 0
                   ? 0
                   : 1;
}
