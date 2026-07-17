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

#include "io/cache/file_cache_common.h"

#if defined(BE_TEST) && defined(BUILD_FILE_CACHE_MICROBENCH_TOOL)

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "io/cache/async_cache_write_service.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "io/fs/file_reader.h"
#include "io/fs/path.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/slice.h"
#include "util/time.h"

DEFINE_string(benchmark_mode, "all",
              "Comma-separated benchmark groups: reader, service, index, or all");
DEFINE_string(cache_path, "./output/async_file_cache_write_microbench",
              "Directory used by the real filesystem-backed BlockFileCache");
DEFINE_uint64(block_size, 1024 * 1024,
              "File-cache block size and stride used by cold reader misses");
DEFINE_uint64(request_size, 64 * 1024, "Bytes returned to the caller by each reader operation");
DEFINE_uint64(reader_operations, 128, "Cold read operations in each sync/async reader case");
DEFINE_uint64(service_task_size, 1024 * 1024, "Payload bytes in each direct service task");
DEFINE_uint64(service_operations, 256, "Attempted tasks in each direct service case");
DEFINE_uint64(service_key_count, 64, "Logical remote files spread across direct service tasks");
DEFINE_uint64(index_operations_per_thread, 100000,
              "Inflight-index lookups performed by each producer thread");
DEFINE_uint64(index_key_count, 4096, "Keys used by the representative sharded index case");
DEFINE_int32(producer_threads, 16, "Concurrent foreground readers or task producers");
DEFINE_int32(reader_workers, 16, "Async write workers used by the reader comparison");
DEFINE_string(worker_counts, "1,4,16",
              "Comma-separated async write worker counts used by service scaling cases");
DEFINE_uint64(repetitions, 5, "Measured repetitions of every selected benchmark case");
DEFINE_uint64(backpressure_pending_tasks, 64,
              "Pending-task limit used by the saturated service case");
DEFINE_uint64(queue_sample_interval_us, 50,
              "Sampling interval for pending, queued, and inflight peak values");
DEFINE_uint64(timeout_seconds, 120, "Maximum time to wait for one benchmark case to drain");
DEFINE_bool(keep_cache, false, "Keep benchmark cache files after the process exits");

namespace doris::io {
namespace {

using Clock = std::chrono::steady_clock;
using Nanoseconds = std::chrono::nanoseconds;

constexpr size_t kMiB = 1024 * 1024;

/// Compact percentile summary for foreground operation latency.
struct LatencySummary {
    double average_us {0};
    double p50_us {0};
    double p95_us {0};
    double p99_us {0};
    double maximum_us {0};
};

/// Return one nearest-rank percentile from an already sorted nanosecond sample set.
/// @param sorted_ns Ascending latency samples in nanoseconds.
/// @param percentile Requested percentile in the inclusive range [0, 1].
double percentile_us(const std::vector<int64_t>& sorted_ns, double percentile) {
    DORIS_CHECK(!sorted_ns.empty());
    DORIS_CHECK(percentile > 0 && percentile <= 1);
    const size_t index =
            static_cast<size_t>(std::ceil(percentile * static_cast<double>(sorted_ns.size()))) - 1;
    return static_cast<double>(sorted_ns[std::min(index, sorted_ns.size() - 1)]) / 1000.0;
}

/// Merge per-thread samples and calculate stable latency percentiles.
/// @param per_thread_ns Independently owned samples, one vector per producer.
LatencySummary summarize_latencies(const std::vector<std::vector<int64_t>>& per_thread_ns) {
    size_t sample_count = 0;
    for (const auto& samples : per_thread_ns) {
        sample_count += samples.size();
    }
    DORIS_CHECK(sample_count > 0);

    std::vector<int64_t> sorted_ns;
    sorted_ns.reserve(sample_count);
    for (const auto& samples : per_thread_ns) {
        sorted_ns.insert(sorted_ns.end(), samples.begin(), samples.end());
    }
    std::sort(sorted_ns.begin(), sorted_ns.end());
    const int64_t total_ns = std::accumulate(sorted_ns.begin(), sorted_ns.end(), int64_t {0});
    return LatencySummary {
            .average_us =
                    static_cast<double>(total_ns) / static_cast<double>(sample_count) / 1000.0,
            .p50_us = percentile_us(sorted_ns, 0.50),
            .p95_us = percentile_us(sorted_ns, 0.95),
            .p99_us = percentile_us(sorted_ns, 0.99),
            .maximum_us = static_cast<double>(sorted_ns.back()) / 1000.0,
    };
}

/// Parse a comma-separated positive integer list used for worker scaling.
/// @param text Raw gflag value such as "1,4,16".
/// @param values Parsed worker counts in input order.
Status parse_positive_integer_list(std::string_view text, std::vector<size_t>* values) {
    DORIS_CHECK(values != nullptr);
    values->clear();
    std::stringstream stream {std::string(text)};
    std::string token;
    while (std::getline(stream, token, ',')) {
        try {
            const long long value = std::stoll(token);
            if (value <= 0) {
                return Status::InvalidArgument("worker count must be positive: {}", token);
            }
            values->push_back(static_cast<size_t>(value));
        } catch (const std::exception& error) {
            return Status::InvalidArgument("invalid worker count '{}': {}", token, error.what());
        }
    }
    if (values->empty()) {
        return Status::InvalidArgument("worker_counts cannot be empty");
    }
    return Status::OK();
}

/// Split the selected benchmark groups while preserving a small command-line surface.
/// @param text Raw mode string.
std::vector<std::string> parse_modes(std::string_view text) {
    std::stringstream stream {std::string(text)};
    std::vector<std::string> modes;
    std::string mode;
    while (std::getline(stream, mode, ',')) {
        if (!mode.empty()) {
            modes.emplace_back(std::move(mode));
        }
    }
    return modes;
}

/// Return whether a benchmark group was requested explicitly or through "all".
/// @param modes Parsed benchmark groups.
/// @param target Group to test.
bool mode_enabled(const std::vector<std::string>& modes, std::string_view target) {
    return std::find(modes.begin(), modes.end(), "all") != modes.end() ||
           std::find(modes.begin(), modes.end(), target) != modes.end();
}

/// Validate sizes and concurrency before allocating cache capacity or starting threads.
Status validate_flags(const std::vector<std::string>& modes,
                      const std::vector<size_t>& worker_counts) {
    if (modes.empty()) {
        return Status::InvalidArgument("benchmark_mode cannot be empty");
    }
    for (const auto& mode : modes) {
        if (mode != "all" && mode != "reader" && mode != "service" && mode != "index") {
            return Status::InvalidArgument("unsupported benchmark mode: {}", mode);
        }
    }
    DORIS_CHECK(!worker_counts.empty());
    if (FLAGS_producer_threads <= 0 || FLAGS_reader_workers <= 0) {
        return Status::InvalidArgument("producer_threads and reader_workers must be positive");
    }
    if (FLAGS_block_size == 0 || FLAGS_request_size == 0 || FLAGS_request_size > FLAGS_block_size) {
        return Status::InvalidArgument("sizes must satisfy 0 < request_size <= block_size");
    }
    if (FLAGS_reader_operations < static_cast<uint64_t>(FLAGS_producer_threads) ||
        FLAGS_service_operations < static_cast<uint64_t>(FLAGS_producer_threads)) {
        return Status::InvalidArgument(
                "reader_operations and service_operations must be at least producer_threads");
    }
    if (FLAGS_service_task_size == 0 || FLAGS_service_task_size > FLAGS_block_size ||
        FLAGS_service_key_count == 0 || FLAGS_index_operations_per_thread == 0 ||
        FLAGS_index_key_count == 0 || FLAGS_backpressure_pending_tasks == 0 ||
        FLAGS_queue_sample_interval_us == 0 || FLAGS_timeout_seconds == 0 ||
        FLAGS_repetitions == 0) {
        return Status::InvalidArgument(
                "operation counts, timeouts, and 0 < service_task_size <= block_size are required");
    }
    return Status::OK();
}

/// Produce deterministic data without adding network or object-store latency to reader results.
class SyntheticRemoteFileReader final : public FileReader {
public:
    /// @param path Stable logical path used to derive the file-cache hash.
    /// @param file_size Virtual file length; no payload is allocated for it.
    /// @param block_size Pattern granularity used to validate copied bytes.
    SyntheticRemoteFileReader(Path path, size_t file_size, size_t block_size)
            : _path(std::move(path)), _file_size(file_size), _block_size(block_size) {}

    Status close() override {
        _closed.store(true, std::memory_order_release);
        return Status::OK();
    }

    const Path& path() const override { return _path; }
    size_t size() const override { return _file_size; }
    bool closed() const override { return _closed.load(std::memory_order_acquire); }
    int64_t mtime() const override { return 0; }

    /// Return the byte value expected at an aligned benchmark block.
    /// @param offset File offset inside the virtual source.
    char expected_byte(size_t offset) const {
        return static_cast<char>((offset / _block_size) % 251);
    }

protected:
    /// Fill the requested span from a deterministic virtual file.
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override {
        DORIS_CHECK(bytes_read != nullptr);
        if (offset > _file_size || result.size > _file_size - offset) {
            return Status::InvalidArgument("synthetic read [{}, {}) exceeds file size {}", offset,
                                           offset + result.size, _file_size);
        }
        size_t copied = 0;
        while (copied < result.size) {
            const size_t current_offset = offset + copied;
            const size_t block_end =
                    std::min(_file_size, (current_offset / _block_size + 1) * _block_size);
            const size_t bytes = std::min(result.size - copied, block_end - current_offset);
            std::memset(result.data + copied, expected_byte(current_offset), bytes);
            copied += bytes;
        }
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    Path _path;
    size_t _file_size;
    size_t _block_size;
    std::atomic<bool> _closed {false};
};

/// Record the first worker-thread error without obscuring the performance hot path.
class ConcurrentError {
public:
    /// Store the first non-OK status observed by any producer.
    void set(Status status) {
        if (status.ok()) {
            return;
        }
        bool expected = false;
        if (_failed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            std::lock_guard lock(_mutex);
            _status = std::move(status);
        }
    }

    bool failed() const { return _failed.load(std::memory_order_acquire); }

    /// Return the stored error after producer threads have joined.
    Status status() const {
        std::lock_guard lock(_mutex);
        return _status;
    }

private:
    std::atomic<bool> _failed {false};
    mutable std::mutex _mutex;
    Status _status;
};

/// Sample public queue gauges so short benchmark cases still report a meaningful high-water mark.
class QueuePeakSampler {
public:
    /// @param service Service whose pending and queued gauges are sampled.
    /// @param index Inflight index paired with the service.
    QueuePeakSampler(AsyncCacheWriteService* service, InflightWriteBufferIndex* index)
            : _service(service), _index(index) {
        DORIS_CHECK(_service != nullptr);
        DORIS_CHECK(_index != nullptr);
    }

    /// Start the sampling thread. The sampler does not mutate benchmark state.
    void start() {
        _running.store(true, std::memory_order_release);
        _thread = std::thread([this]() {
            while (_running.load(std::memory_order_acquire)) {
                update_max(&_peak_pending, _service->pending_count());
                update_max(&_peak_queued, _service->queued_count());
                update_max(&_peak_inflight, _index->size());
                std::this_thread::sleep_for(
                        std::chrono::microseconds(FLAGS_queue_sample_interval_us));
            }
            update_max(&_peak_pending, _service->pending_count());
            update_max(&_peak_queued, _service->queued_count());
            update_max(&_peak_inflight, _index->size());
        });
    }

    /// Stop sampling and join before reading peak values.
    void stop() {
        _running.store(false, std::memory_order_release);
        if (_thread.joinable()) {
            _thread.join();
        }
    }

    ~QueuePeakSampler() { stop(); }

    size_t peak_pending() const { return _peak_pending.load(std::memory_order_relaxed); }
    size_t peak_queued() const { return _peak_queued.load(std::memory_order_relaxed); }
    size_t peak_inflight() const { return _peak_inflight.load(std::memory_order_relaxed); }

private:
    /// Atomically preserve the largest sampled gauge value.
    static void update_max(std::atomic<size_t>* maximum, size_t value) {
        size_t current = maximum->load(std::memory_order_relaxed);
        while (current < value &&
               !maximum->compare_exchange_weak(current, value, std::memory_order_relaxed)) {
        }
    }

    AsyncCacheWriteService* _service;
    InflightWriteBufferIndex* _index;
    std::atomic<bool> _running {false};
    std::thread _thread;
    std::atomic<size_t> _peak_pending {0};
    std::atomic<size_t> _peak_queued {0};
    std::atomic<size_t> _peak_inflight {0};
};

/// Poll one asynchronous completion predicate with a bounded failure mode.
/// @param predicate Returns true after the expected state is reached.
/// @param description Included in timeout diagnostics.
template <typename Predicate>
Status wait_until(Predicate&& predicate, std::string_view description) {
    const auto deadline = Clock::now() + std::chrono::seconds(FLAGS_timeout_seconds);
    while (!predicate()) {
        if (Clock::now() >= deadline) {
            return Status::TimedOut("timed out waiting for {}", description);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    return Status::OK();
}

/// Own one real filesystem cache and install its factory into the otherwise minimal ExecEnv.
class BenchmarkEnvironment {
public:
    BenchmarkEnvironment() = default;

    ~BenchmarkEnvironment() {
        if (_factory) {
            ExecEnv::GetInstance()->set_file_cache_factory(nullptr);
            _factory.reset();
        }
        if (_owns_cache_path && !FLAGS_keep_cache) {
            std::error_code error;
            std::filesystem::remove_all(FLAGS_cache_path, error);
        }
    }

    /// Configure globals, create the cache, and wait for its asynchronous metadata open.
    /// @param cache_capacity Bytes reserved for the benchmark's normal queue.
    Status initialize(size_t cache_capacity) {
        std::error_code error;
        const auto cache_path =
                std::filesystem::absolute(FLAGS_cache_path, error).lexically_normal();
        if (error) {
            return Status::IOError("failed to resolve cache path {}: {}", FLAGS_cache_path,
                                   error.message());
        }
        const auto current_path = std::filesystem::current_path(error);
        if (error) {
            return Status::IOError("failed to resolve current directory: {}", error.message());
        }
        if (cache_path == cache_path.root_path() || cache_path == current_path) {
            return Status::InvalidArgument("cache path must be a dedicated subdirectory: {}",
                                           cache_path.string());
        }
        const bool cache_path_exists = std::filesystem::exists(cache_path, error);
        if (error) {
            return Status::IOError("failed to inspect cache path {}: {}", cache_path.string(),
                                   error.message());
        }
        if (cache_path_exists) {
            const bool cache_path_is_directory = std::filesystem::is_directory(cache_path, error);
            if (error) {
                return Status::IOError("failed to inspect cache path {}: {}", cache_path.string(),
                                       error.message());
            }
            if (!cache_path_is_directory) {
                return Status::InvalidArgument("cache path is not a directory: {}",
                                               cache_path.string());
            }
            const bool cache_path_is_empty = std::filesystem::is_empty(cache_path, error);
            if (error) {
                return Status::IOError("failed to inspect cache path {}: {}", cache_path.string(),
                                       error.message());
            }
            if (!cache_path_is_empty) {
                return Status::InvalidArgument(
                        "cache path must not exist or must be an empty directory: {}",
                        cache_path.string());
            }
        } else {
            std::filesystem::create_directories(cache_path, error);
            if (error) {
                return Status::IOError("failed to create cache path {}: {}", cache_path.string(),
                                       error.message());
            }
        }
        _owns_cache_path = true;

        config::enable_async_file_cache_write = true;
        config::enable_async_file_cache_write_inflight_write_buffer_index = true;
        config::enable_read_cache_file_directly = false;
        config::enable_cache_read_from_peer = false;
        config::clear_file_cache = true;
        config::enable_evict_file_cache_in_advance = false;
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        config::file_cache_each_block_size = static_cast<int64_t>(FLAGS_block_size);
        // Benchmark reads are external-table style with tablet_id=0, so they do not register
        // tablet-scoped TTL work. Short intervals only bound cache teardown latency; production
        // defaults let the otherwise idle TTL threads sleep for three minutes before join().
        config::file_cache_background_ttl_gc_interval_ms = 100;
        config::file_cache_background_ttl_info_update_interval_ms = 100;
        config::file_cache_background_tablet_id_flush_interval_ms = 100;
        config::async_file_cache_write_workers_per_disk = FLAGS_reader_workers;
        config::async_file_cache_write_max_pending_tasks_per_disk =
                static_cast<int64_t>(std::max(FLAGS_reader_operations, FLAGS_service_operations));
        config::async_file_cache_write_batch_size = 16;

        DORIS_CHECK(ExecEnv::GetInstance()->file_cache_factory() == nullptr);
        ExecEnv::GetInstance()->set_file_cache_open_fd_cache(std::make_unique<FDCache>());
        _factory = std::make_unique<FileCacheFactory>();
        ExecEnv::GetInstance()->set_file_cache_factory(_factory.get());

        FileCacheSettings settings;
        settings.capacity = cache_capacity;
        settings.max_file_block_size = FLAGS_block_size;
        settings.max_query_cache_size = 0;
        const size_t auxiliary_queue_size = FLAGS_block_size;
        settings.disposable_queue_size = auxiliary_queue_size;
        settings.disposable_queue_elements = 128;
        settings.index_queue_size = auxiliary_queue_size;
        settings.index_queue_elements = 128;
        settings.ttl_queue_size = auxiliary_queue_size;
        settings.ttl_queue_elements = 128;
        settings.query_queue_size = cache_capacity - 3 * auxiliary_queue_size;
        settings.query_queue_elements = std::max<size_t>(
                1024, 2 * std::max(FLAGS_reader_operations, FLAGS_service_operations));

        RETURN_IF_ERROR(_factory->create_file_cache(FLAGS_cache_path, settings));
        _cache = _factory->get_by_path(FLAGS_cache_path);
        DORIS_CHECK(_cache != nullptr);
        RETURN_IF_ERROR(wait_until([&]() { return _cache->get_async_open_success(); },
                                   "file cache initialization"));
        return Status::OK();
    }

    /// Drain accepted writes, then synchronously invalidate and clear all cached blocks.
    Status clear_cache() {
        RETURN_IF_ERROR(wait_for_idle());
        std::string clear_result;
        RETURN_IF_ERROR(_factory->clear_file_caches(true, &clear_result));
        return wait_for_idle();
    }

    /// Apply one explicit worker/queue snapshot to the production service.
    /// @param workers Active background writer count.
    /// @param max_pending Maximum accepted tasks, including active workers.
    Status configure_service(size_t workers, size_t max_pending) {
        auto options = service()->options();
        options.worker_count = workers;
        options.max_pending_tasks = max_pending;
        options.batch_size = 16;
        options.watchdog_warn_secs = static_cast<int64_t>(FLAGS_timeout_seconds);
        options.watchdog_drop_secs = static_cast<int64_t>(FLAGS_timeout_seconds * 2);
        return service()->update_options(options);
    }

    /// Wait until both queue ownership and reader-visible inflight payloads are gone.
    Status wait_for_idle() {
        return wait_until(
                [&]() {
                    return service()->pending_count() == 0 && service()->queued_count() == 0 &&
                           index()->size() == 0;
                },
                "async write queue and inflight index to drain");
    }

    BlockFileCache* cache() const { return _cache; }
    AsyncCacheWriteService* service() const { return _cache->async_write_service(); }
    InflightWriteBufferIndex* index() const { return _cache->inflight_write_buffer_index(); }

private:
    std::unique_ptr<FileCacheFactory> _factory;
    BlockFileCache* _cache {nullptr};
    bool _owns_cache_path {false};
};

/// Verify that a complete range can be resolved from the final cache state.
/// @param cache Target cache.
/// @param hash Logical file hash.
/// @param offset Range offset to verify.
/// @param size Expected persisted bytes.
Status verify_cached_range(BlockFileCache* cache, const UInt128Wrapper& hash, size_t offset,
                           size_t size) {
    DORIS_CHECK(cache != nullptr);
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    FileBlocks blocks;
    bool fully_covered = false;
    RETURN_IF_ERROR(cache->get_downloaded_blocks_if_fully_covered(hash, offset, size, context,
                                                                  &blocks, &fully_covered));
    if (!fully_covered) {
        return Status::InternalError("cache range [{}, {}) was not persisted", offset,
                                     offset + size);
    }
    return Status::OK();
}

/// Shared result fields printed for reader and direct-service cases.
struct AsyncWriteResult {
    std::string benchmark;
    std::string variant;
    size_t producers {0};
    size_t workers {0};
    size_t operations {0};
    size_t accepted {0};
    size_t rejected {0};
    size_t persisted {0};
    size_t bytes_per_operation {0};
    double foreground_seconds {0};
    double drain_seconds {0};
    double total_seconds {0};
    size_t peak_pending {0};
    size_t peak_queued {0};
    size_t peak_inflight {0};
    LatencySummary latency;
};

/// Print one machine-readable line without hiding the foreground/drain distinction.
/// @param result Completed benchmark result.
/// @param repetition One-based repetition index.
void print_async_write_result(const AsyncWriteResult& result, size_t repetition) {
    const double foreground_ops_per_sec =
            static_cast<double>(result.operations) / result.foreground_seconds;
    const double persisted_mib_per_sec =
            result.total_seconds > 0
                    ? static_cast<double>(result.persisted * result.bytes_per_operation) /
                              static_cast<double>(kMiB) / result.total_seconds
                    : 0;
    std::cout << std::fixed << std::setprecision(3) << "RESULT"
              << " benchmark=" << result.benchmark << " variant=" << result.variant
              << " repetition=" << repetition << " producers=" << result.producers
              << " workers=" << result.workers << " operations=" << result.operations
              << " accepted=" << result.accepted << " rejected=" << result.rejected
              << " persisted=" << result.persisted
              << " bytes_per_operation=" << result.bytes_per_operation
              << " foreground_seconds=" << result.foreground_seconds
              << " drain_seconds=" << result.drain_seconds
              << " total_seconds=" << result.total_seconds
              << " foreground_ops_per_sec=" << foreground_ops_per_sec
              << " persisted_mib_per_sec=" << persisted_mib_per_sec
              << " avg_us=" << result.latency.average_us << " p50_us=" << result.latency.p50_us
              << " p95_us=" << result.latency.p95_us << " p99_us=" << result.latency.p99_us
              << " max_us=" << result.latency.maximum_us << " peak_pending=" << result.peak_pending
              << " peak_queued=" << result.peak_queued << " peak_inflight=" << result.peak_inflight
              << '\n';
}

/// Compare cold-miss caller latency with synchronous and asynchronous cache persistence.
/// @param environment Shared real cache, cleared before the case.
/// @param mode Explicit write policy applied to every CachedRemoteFileReader.
/// @param variant Stable output label.
/// @param repetition One-based repetition index included in output.
Status run_reader_case(BenchmarkEnvironment* environment, CacheWriteMode mode, std::string variant,
                       size_t repetition) {
    DORIS_CHECK(environment != nullptr);
    RETURN_IF_ERROR(environment->clear_cache());
    RETURN_IF_ERROR(environment->configure_service(
            static_cast<size_t>(FLAGS_reader_workers),
            static_cast<size_t>(FLAGS_reader_operations + FLAGS_reader_workers)));

    const size_t producer_count = static_cast<size_t>(FLAGS_producer_threads);
    const size_t operation_count = static_cast<size_t>(FLAGS_reader_operations);
    const size_t file_size = operation_count * FLAGS_block_size;
    const std::string file_name = "async_write_reader_" + variant + ".bin";
    const Path path("/synthetic/" + file_name);

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    std::vector<std::vector<int64_t>> latencies(producer_count);
    std::vector<FileCacheStatistics> statistics(producer_count);
    std::barrier start_barrier(static_cast<std::ptrdiff_t>(producer_count + 1));
    ConcurrentError error;
    QueuePeakSampler sampler(environment->service(), environment->index());
    sampler.start();

    for (size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer]() {
            SCOPED_INIT_THREAD_CONTEXT();
            auto remote =
                    std::make_shared<SyntheticRemoteFileReader>(path, file_size, FLAGS_block_size);
            FileReaderOptions options;
            options.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
            options.cache_write_mode = mode;
            options.cache_base_path = FLAGS_cache_path;
            options.tablet_id = 0;
            auto reader = std::make_shared<CachedRemoteFileReader>(remote, options);
            IOContext io_context;
            io_context.file_cache_stats = &statistics[producer];
            io_context.bypass_peer_read = true;
            std::string buffer(FLAGS_request_size, '\0');
            latencies[producer].reserve((operation_count + producer_count - 1) / producer_count);

            start_barrier.arrive_and_wait();
            for (size_t operation = producer; operation < operation_count;
                 operation += producer_count) {
                if (error.failed()) {
                    break;
                }
                const size_t offset = operation * FLAGS_block_size;
                size_t bytes_read = 0;
                const auto start = Clock::now();
                Status status = reader->read_at(offset, Slice(buffer.data(), buffer.size()),
                                                &bytes_read, &io_context);
                const auto elapsed = std::chrono::duration_cast<Nanoseconds>(Clock::now() - start);
                latencies[producer].push_back(elapsed.count());
                if (!status.ok()) {
                    error.set(std::move(status));
                    break;
                }
                if (bytes_read != buffer.size() ||
                    buffer.front() != remote->expected_byte(offset) ||
                    buffer.back() != remote->expected_byte(offset + buffer.size() - 1)) {
                    error.set(Status::InternalError(
                            "reader result mismatch at operation {}, bytes_read={}", operation,
                            bytes_read));
                    break;
                }
            }
        });
    }

    const auto foreground_start = Clock::now();
    start_barrier.arrive_and_wait();
    for (auto& producer : producers) {
        producer.join();
    }
    const auto foreground_end = Clock::now();
    if (error.failed()) {
        sampler.stop();
        return error.status();
    }

    RETURN_IF_ERROR(environment->wait_for_idle());
    const auto drain_end = Clock::now();
    sampler.stop();

    FileCacheStatistics total_stats;
    for (const auto& local_stats : statistics) {
        total_stats.merge_from(local_stats);
    }
    const auto hash = BlockFileCache::hash(path.native() + ":0");
    RETURN_IF_ERROR(verify_cached_range(environment->cache(), hash, 0, file_size));

    const double foreground_seconds =
            std::chrono::duration<double>(foreground_end - foreground_start).count();
    const double drain_seconds = std::chrono::duration<double>(drain_end - foreground_end).count();
    AsyncWriteResult result {
            .benchmark = "reader",
            .variant = std::move(variant),
            .producers = producer_count,
            .workers = mode == CacheWriteMode::ASYNC_WRITE
                               ? static_cast<size_t>(FLAGS_reader_workers)
                               : 0,
            .operations = operation_count,
            .accepted = mode == CacheWriteMode::ASYNC_WRITE
                                ? static_cast<size_t>(total_stats.async_cache_write_submitted)
                                : 0,
            .rejected = static_cast<size_t>(total_stats.async_cache_write_rejected),
            .persisted = operation_count,
            .bytes_per_operation = FLAGS_block_size,
            .foreground_seconds = foreground_seconds,
            .drain_seconds = drain_seconds,
            .total_seconds = foreground_seconds + drain_seconds,
            .peak_pending = sampler.peak_pending(),
            .peak_queued = sampler.peak_queued(),
            .peak_inflight = sampler.peak_inflight(),
            .latency = summarize_latencies(latencies),
    };
    print_async_write_result(result, repetition);
    return Status::OK();
}

/// One accepted direct-service task retained for post-timing persistence verification.
struct ServiceTaskRecord {
    UInt128Wrapper hash;
    size_t offset {0};
    size_t size {0};
};

/// Exercise producer admission, inflight publication, MPMC consumption, and real persistence.
/// @param environment Shared real cache, cleared before the case.
/// @param variant Stable output label.
/// @param workers Active service consumers.
/// @param max_pending Bounded pending-task limit.
/// @param repetition One-based repetition index included in output.
Status run_service_case(BenchmarkEnvironment* environment, std::string variant, size_t workers,
                        size_t max_pending, size_t repetition) {
    DORIS_CHECK(environment != nullptr);
    RETURN_IF_ERROR(environment->clear_cache());
    RETURN_IF_ERROR(environment->configure_service(workers, max_pending));

    const size_t producer_count = static_cast<size_t>(FLAGS_producer_threads);
    const size_t operation_count = static_cast<size_t>(FLAGS_service_operations);
    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    std::vector<std::vector<int64_t>> latencies(producer_count);
    std::vector<std::vector<ServiceTaskRecord>> accepted_records(producer_count);
    std::barrier start_barrier(static_cast<std::ptrdiff_t>(producer_count + 1));
    std::atomic<size_t> accepted {0};
    std::atomic<size_t> rejected {0};
    std::atomic<size_t> completed {0};
    ConcurrentError error;
    QueuePeakSampler sampler(environment->service(), environment->index());
    sampler.start();

    for (size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer]() {
            SCOPED_INIT_THREAD_CONTEXT();
            latencies[producer].reserve((operation_count + producer_count - 1) / producer_count);
            accepted_records[producer].reserve((operation_count + producer_count - 1) /
                                               producer_count);
            start_barrier.arrive_and_wait();

            for (size_t operation = producer; operation < operation_count;
                 operation += producer_count) {
                if (error.failed()) {
                    break;
                }
                const size_t key_index = operation % FLAGS_service_key_count;
                const size_t block_index = operation / FLAGS_service_key_count;
                const auto hash = BlockFileCache::hash("async_write_service_" + variant + "_" +
                                                       std::to_string(key_index));
                const size_t offset = block_index * FLAGS_block_size;
                const auto start = Clock::now();

                AsyncCacheWriteBufferPtr buffer;
                Status status = environment->service()->allocate_tracked_buffer(
                        FLAGS_service_task_size, &buffer);
                if (!status.ok()) {
                    error.set(std::move(status));
                    break;
                }
                std::memset(buffer->data(), static_cast<int>(operation % 251), buffer->size());
                const uint64_t epoch = environment->service()->current_write_epoch();
                auto entry = std::make_shared<InflightWriteBufferEntry>(
                        buffer, offset, buffer->size(), MonotonicMicros(), epoch);
                auto existing = environment->index()->insert_if_absent(hash, offset, entry);
                if (existing != nullptr) {
                    error.set(Status::InternalError(
                            "unexpected duplicate inflight owner at operation {}", operation));
                    break;
                }

                AsyncCacheWriteTask task {
                        .cache_hash = hash,
                        .file_offset = offset,
                        .file_size = buffer->size(),
                        .buffer = buffer,
                        .admission_ctx = {},
                        .submit_ts_us = MonotonicMicros(),
                        .write_epoch = epoch,
                        .on_finalized =
                                [index = environment->index(), hash, offset, entry,
                                 &completed](const AsyncCacheWriteTask&) {
                                    index->remove_if(hash, offset, entry);
                                    completed.fetch_add(1, std::memory_order_release);
                                },
                };
                const bool submitted = environment->service()->try_submit(std::move(task));
                const auto elapsed = std::chrono::duration_cast<Nanoseconds>(Clock::now() - start);
                latencies[producer].push_back(elapsed.count());
                if (submitted) {
                    accepted.fetch_add(1, std::memory_order_relaxed);
                    accepted_records[producer].push_back(ServiceTaskRecord {
                            .hash = hash, .offset = offset, .size = FLAGS_service_task_size});
                } else {
                    environment->index()->remove_if(hash, offset, entry);
                    environment->index()->record_backpressure_rollback();
                    rejected.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    const auto foreground_start = Clock::now();
    start_barrier.arrive_and_wait();
    for (auto& producer : producers) {
        producer.join();
    }
    const auto foreground_end = Clock::now();
    if (error.failed()) {
        sampler.stop();
        return error.status();
    }

    RETURN_IF_ERROR(wait_until(
            [&]() {
                return environment->service()->pending_count() == 0 &&
                       completed.load(std::memory_order_acquire) ==
                               accepted.load(std::memory_order_acquire) &&
                       environment->index()->size() == 0;
            },
            "direct service tasks to complete"));
    const auto drain_end = Clock::now();
    sampler.stop();

    size_t persisted = 0;
    for (const auto& records : accepted_records) {
        for (const auto& record : records) {
            Status status = verify_cached_range(environment->cache(), record.hash, record.offset,
                                                record.size);
            if (status.ok()) {
                ++persisted;
            }
        }
    }
    if (persisted != accepted.load(std::memory_order_relaxed)) {
        return Status::InternalError("only {} of {} accepted service tasks persisted", persisted,
                                     accepted.load(std::memory_order_relaxed));
    }

    const double foreground_seconds =
            std::chrono::duration<double>(foreground_end - foreground_start).count();
    const double drain_seconds = std::chrono::duration<double>(drain_end - foreground_end).count();
    AsyncWriteResult result {
            .benchmark = "service",
            .variant = std::move(variant),
            .producers = producer_count,
            .workers = workers,
            .operations = operation_count,
            .accepted = accepted.load(std::memory_order_relaxed),
            .rejected = rejected.load(std::memory_order_relaxed),
            .persisted = persisted,
            .bytes_per_operation = FLAGS_service_task_size,
            .foreground_seconds = foreground_seconds,
            .drain_seconds = drain_seconds,
            .total_seconds = foreground_seconds + drain_seconds,
            .peak_pending = sampler.peak_pending(),
            .peak_queued = sampler.peak_queued(),
            .peak_inflight = sampler.peak_inflight(),
            .latency = summarize_latencies(latencies),
    };
    print_async_write_result(result, repetition);
    return Status::OK();
}

/// Print one inflight-index result using the same latency field names as write cases.
/// @param repetition One-based repetition index.
void print_index_result(std::string_view variant, size_t repetition, size_t operations,
                        double elapsed_seconds, const LatencySummary& latency) {
    std::cout << std::fixed << std::setprecision(3) << "RESULT benchmark=index"
              << " variant=" << variant << " repetition=" << repetition
              << " producers=" << FLAGS_producer_threads << " operations=" << operations
              << " elapsed_seconds=" << elapsed_seconds
              << " operations_per_sec=" << static_cast<double>(operations) / elapsed_seconds
              << " avg_us=" << latency.average_us << " p50_us=" << latency.p50_us
              << " p95_us=" << latency.p95_us << " p99_us=" << latency.p99_us
              << " max_us=" << latency.maximum_us << '\n';
}

/// Measure either representative sharded misses or worst-case single-key hit contention.
/// @param variant Stable output label.
/// @param key_count Number of distinct cache hashes used by the workload.
/// @param populate Whether every lookup should hit a pre-populated entry.
/// @param repetition One-based repetition index included in output.
Status run_index_case(std::string variant, size_t key_count, bool populate, size_t repetition) {
    const size_t producer_count = static_cast<size_t>(FLAGS_producer_threads);
    const size_t operations_per_thread = static_cast<size_t>(FLAGS_index_operations_per_thread);
    InflightWriteBufferIndex index(
            64, FLAGS_cache_path + "_index_" + variant + "_" + std::to_string(repetition));
    std::vector<UInt128Wrapper> hashes;
    hashes.reserve(key_count);
    const uint64_t epoch = 1;
    auto entry =
            std::make_shared<InflightWriteBufferEntry>(nullptr, 0, 1, MonotonicMicros(), epoch);
    for (size_t key = 0; key < key_count; ++key) {
        hashes.emplace_back(
                BlockFileCache::hash("inflight_index_" + variant + "_" + std::to_string(key)));
        if (populate) {
            DORIS_CHECK(index.insert_if_absent(hashes.back(), 0, entry) == nullptr);
        }
    }

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    std::vector<std::vector<int64_t>> latencies(producer_count);
    std::barrier start_barrier(static_cast<std::ptrdiff_t>(producer_count + 1));
    ConcurrentError error;
    for (size_t producer = 0; producer < producer_count; ++producer) {
        producers.emplace_back([&, producer]() {
            latencies[producer].reserve(operations_per_thread);
            start_barrier.arrive_and_wait();
            for (size_t operation = 0; operation < operations_per_thread; ++operation) {
                const size_t key = (operation * 2654435761ULL + producer) % key_count;
                const auto start = Clock::now();
                auto result = index.lookup(hashes[key], 0, epoch);
                const auto elapsed = std::chrono::duration_cast<Nanoseconds>(Clock::now() - start);
                latencies[producer].push_back(elapsed.count());
                if ((result != nullptr) != populate) {
                    error.set(Status::InternalError("unexpected index lookup result"));
                    break;
                }
            }
        });
    }

    const auto start = Clock::now();
    start_barrier.arrive_and_wait();
    for (auto& producer : producers) {
        producer.join();
    }
    const auto end = Clock::now();
    if (error.failed()) {
        return error.status();
    }

    const size_t total_operations = producer_count * operations_per_thread;
    print_index_result(variant, repetition, total_operations,
                       std::chrono::duration<double>(end - start).count(),
                       summarize_latencies(latencies));
    return Status::OK();
}

/// Run the selected benchmark groups in a fixed, directly comparable order.
Status run_benchmarks(const std::vector<std::string>& modes,
                      const std::vector<size_t>& worker_counts) {
    const size_t reader_bytes = FLAGS_reader_operations * FLAGS_block_size;
    const size_t service_bytes = FLAGS_service_operations * FLAGS_service_task_size;
    const size_t workload_bytes = std::max(reader_bytes, service_bytes);
    const size_t cache_capacity = std::max<size_t>(256 * kMiB, workload_bytes * 4);
    if (cache_capacity <= 3 * FLAGS_block_size) {
        return Status::InvalidArgument("calculated cache capacity is too small");
    }

    BenchmarkEnvironment environment;
    RETURN_IF_ERROR(environment.initialize(cache_capacity));
    std::cout << "CONFIG cache_path=" << FLAGS_cache_path << " cache_capacity=" << cache_capacity
              << " block_size=" << FLAGS_block_size << " request_size=" << FLAGS_request_size
              << " producer_threads=" << FLAGS_producer_threads
              << " repetitions=" << FLAGS_repetitions << '\n';

    for (size_t repetition = 1; repetition <= FLAGS_repetitions; ++repetition) {
        if (mode_enabled(modes, "reader")) {
            RETURN_IF_ERROR(run_reader_case(&environment, CacheWriteMode::SYNC_WRITE, "sync_write",
                                            repetition));
            RETURN_IF_ERROR(run_reader_case(&environment, CacheWriteMode::ASYNC_WRITE,
                                            "async_write", repetition));
        }
        if (mode_enabled(modes, "service")) {
            for (size_t workers : worker_counts) {
                RETURN_IF_ERROR(run_service_case(
                        &environment, "drain_workers_" + std::to_string(workers), workers,
                        static_cast<size_t>(FLAGS_service_operations + workers), repetition));
            }
            RETURN_IF_ERROR(run_service_case(
                    &environment, "backpressure", worker_counts.back(),
                    std::min<size_t>(FLAGS_backpressure_pending_tasks, FLAGS_service_operations),
                    repetition));
        }
        if (mode_enabled(modes, "index")) {
            RETURN_IF_ERROR(
                    run_index_case("sharded_miss", FLAGS_index_key_count, false, repetition));
            RETURN_IF_ERROR(run_index_case("sharded_hit", FLAGS_index_key_count, true, repetition));
            RETURN_IF_ERROR(run_index_case("hot_key_hit", 1, true, repetition));
        }
    }
    return Status::OK();
}

} // namespace
} // namespace doris::io

int main(int argc, char** argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = true;
    // Keep RESULT lines parseable when the documented runner merges stdout and stderr.
    FLAGS_minloglevel = google::GLOG_ERROR;
    google::InitGoogleLogging(argv[0]);

    // Doris configuration defaults contain ${DORIS_HOME}; a standalone benchmark has no launcher
    // to provide it, and only needs a valid expansion rather than a configuration file.
    if (std::getenv("DORIS_HOME") == nullptr) {
        const std::string current_directory = std::filesystem::current_path().string();
        if (::setenv("DORIS_HOME", current_directory.c_str(), 0) != 0) {
            std::cerr << "Benchmark failed: cannot initialize DORIS_HOME\n";
            return 1;
        }
    }
    if (!doris::config::init(nullptr, true)) {
        std::cerr << "Benchmark failed: cannot initialize Doris configuration defaults\n";
        return 1;
    }
    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();
    SCOPED_INIT_THREAD_CONTEXT();
    doris::ExecEnv::GetInstance()->init_mem_tracker();
    DORIS_CHECK(doris::thread_context()->thread_mem_tracker_mgr->init());

    std::vector<size_t> worker_counts;
    doris::Status status =
            doris::io::parse_positive_integer_list(FLAGS_worker_counts, &worker_counts);
    const auto modes = doris::io::parse_modes(FLAGS_benchmark_mode);
    if (status.ok()) {
        status = doris::io::validate_flags(modes, worker_counts);
    }
    if (status.ok()) {
        status = doris::io::run_benchmarks(modes, worker_counts);
    }
    if (!status.ok()) {
        std::cerr << "Benchmark failed: " << status.to_string() << '\n';
        return 1;
    }
    return 0;
}

#endif
