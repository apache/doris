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

#include "exec/spill/spill_file_manager.h"

#include <bvar/bvar.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <glog/logging.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "cloud/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "exec/spill/remote_spill_data_dir.h"
#include "exec/spill/spill_file.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "util/debug_points.h"
#include "util/pretty_printer.h"
#include "util/time.h"

namespace doris {

// BE-wide object storage traffic of spill. The PerSecond windows give QPS and bytes/s directly.
bvar::Adder<int64_t> g_spill_remote_read_bytes("spill_remote_read_bytes");
bvar::Adder<int64_t> g_spill_remote_write_bytes("spill_remote_write_bytes");
bvar::Adder<int64_t> g_spill_remote_get_requests("spill_remote_get_requests");
bvar::Adder<int64_t> g_spill_remote_put_requests("spill_remote_put_requests");
bvar::PerSecond<bvar::Adder<int64_t>> g_spill_remote_read_throughput("spill_remote_read_throughput",
                                                                     &g_spill_remote_read_bytes,
                                                                     10);
bvar::PerSecond<bvar::Adder<int64_t>> g_spill_remote_write_throughput(
        "spill_remote_write_throughput", &g_spill_remote_write_bytes, 10);
bvar::PerSecond<bvar::Adder<int64_t>> g_spill_remote_get_qps("spill_remote_get_qps",
                                                             &g_spill_remote_get_requests, 10);
bvar::PerSecond<bvar::Adder<int64_t>> g_spill_remote_put_qps("spill_remote_put_qps",
                                                             &g_spill_remote_put_requests, 10);

SpillFileManager::~SpillFileManager() {
    // QueryContext destruction can still queue failed deletions after stop(), for example while
    // VDataStreamMgr is being destroyed. Retry them once more before dropping the in-memory state.
    // Any directory that still cannot be deleted remains under the active spill root and will be
    // moved to the GC root by init() after restart.
    _retry_pending_query_spill_directories();
    DorisMetrics::instance()->metric_registry()->deregister_entity(_entity);
}

SpillFileManager::SpillFileManager(
        std::unordered_map<std::string, std::unique_ptr<SpillDataDir>>&& spill_store_map)
        : _spill_store_map(std::move(spill_store_map)), _stop_background_threads_latch(1) {
    for (auto& [path, store] : _spill_store_map) {
        if (store->is_remote()) {
            auto* remote = dynamic_cast<RemoteSpillDataDir*>(store.get());
            DCHECK(remote != nullptr) << "remote spill store must be a RemoteSpillDataDir";
            DCHECK(_remote_store == nullptr) << "at most one remote spill store";
            _remote_store = remote;
        } else {
            auto* local = dynamic_cast<LocalSpillDataDir*>(store.get());
            DCHECK(local != nullptr) << "local spill store must be a LocalSpillDataDir";
            _local_stores.push_back(local);
        }
    }
}

void SpillFileManager::stop() {
    _stop_background_threads_latch.count_down();
    if (_spill_gc_thread) {
        _spill_gc_thread->join();
    }
    // The GC thread may observe the stop latch before processing a recently queued failed deletion.
    // Retry the pending directories after the thread exits; later failures get one final retry in
    // the destructor.
    _retry_pending_query_spill_directories();
}

Status SpillFileManager::init() {
    LOG(INFO) << "init spill stream manager";
    RETURN_IF_ERROR(_init_spill_store_map());
    _remote_upload_budget =
            std::make_shared<SpillRemoteUploadBudget>(config::spill_s3_max_inflight_upload_bytes);

    if (_remote_store != nullptr) {
        // Objects of previous boot generations are deleted by the GC thread once the store
        // is ready. Nothing here may touch meta-service: BE has not received the FE
        // heartbeat yet, so the backend id and the storage vault may be unavailable.
        _remote_boot_marker_pending.store(true, std::memory_order_release);
        _remote_startup_cleanup_pending.store(true, std::memory_order_release);
    }
    for (auto* store : _local_stores) {
        auto gc_dir_root_dir = store->get_spill_data_gc_path();
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(gc_dir_root_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(gc_dir_root_dir));
        }

        auto spill_dir = store->get_spill_data_path();
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(spill_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(spill_dir));
        } else {
            auto suffix = ToStringFromUnixMillis(UnixMillis());
            auto gc_dir = store->get_spill_data_gc_path(suffix);
            if (std::filesystem::exists(gc_dir)) {
                LOG(WARNING) << "gc dir already exists: " << gc_dir;
            }
            (void)io::global_local_filesystem()->rename(spill_dir, gc_dir);
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(spill_dir));
        }
    }

    RETURN_IF_ERROR(Thread::create(
            "Spill", "spill_gc_thread", [this]() { this->_spill_gc_thread_callback(); },
            &_spill_gc_thread));
    LOG(INFO) << "spill gc thread started";

    _init_metrics();

    return Status::OK();
}

void SpillFileManager::_init_metrics() {
    _entity = DorisMetrics::instance()->metric_registry()->register_entity("spill",
                                                                           {{"name", "spill"}});

    _spill_write_bytes_metric = std::make_unique<doris::MetricPrototype>(
            doris::MetricType::COUNTER, doris::MetricUnit::BYTES, "spill_write_bytes");
    _spill_write_bytes_counter = (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(
            _spill_write_bytes_metric.get()));

    _spill_read_bytes_metric = std::make_unique<doris::MetricPrototype>(
            doris::MetricType::COUNTER, doris::MetricUnit::BYTES, "spill_read_bytes");
    _spill_read_bytes_counter = (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(
            _spill_read_bytes_metric.get()));

    auto register_counter = [&](std::unique_ptr<doris::MetricPrototype>& proto,
                                doris::MetricUnit unit, const char* name) {
        proto = std::make_unique<doris::MetricPrototype>(doris::MetricType::COUNTER, unit, name);
        return (IntAtomicCounter*)(_entity->register_metric<IntAtomicCounter>(proto.get()));
    };
    auto register_gauge = [&](std::unique_ptr<doris::MetricPrototype>& proto,
                              doris::MetricUnit unit, const char* name) {
        proto = std::make_unique<doris::MetricPrototype>(doris::MetricType::GAUGE, unit, name);
        return (IntGauge*)(_entity->register_metric<IntGauge>(proto.get()));
    };
    _spill_remote_write_bytes_counter = register_counter(
            _spill_remote_write_bytes_metric, doris::MetricUnit::BYTES, "spill_remote_write_bytes");
    _spill_remote_read_bytes_counter = register_counter(
            _spill_remote_read_bytes_metric, doris::MetricUnit::BYTES, "spill_remote_read_bytes");
    _spill_remote_put_requests_counter =
            register_counter(_spill_remote_put_requests_metric, doris::MetricUnit::REQUESTS,
                             "spill_remote_put_requests");
    _spill_remote_get_requests_counter =
            register_counter(_spill_remote_get_requests_metric, doris::MetricUnit::REQUESTS,
                             "spill_remote_get_requests");
    _spill_pending_delete_dir_count_gauge =
            register_gauge(_spill_pending_delete_dir_count_metric, doris::MetricUnit::NOUNIT,
                           "spill_pending_delete_dir_count");
    _spill_remote_inflight_upload_bytes_gauge =
            register_gauge(_spill_remote_inflight_upload_bytes_metric, doris::MetricUnit::BYTES,
                           "spill_remote_inflight_upload_bytes");
    _spill_remote_startup_cleanup_pending_gauge =
            register_gauge(_spill_remote_startup_cleanup_pending_metric, doris::MetricUnit::NOUNIT,
                           "spill_remote_startup_cleanup_pending");
}

void SpillFileManager::update_spill_remote_write(int64_t bytes, int64_t put_requests) {
    g_spill_remote_write_bytes << bytes;
    g_spill_remote_put_requests << put_requests;
    if (_spill_remote_write_bytes_counter != nullptr) {
        _spill_remote_write_bytes_counter->increment(bytes);
        _spill_remote_put_requests_counter->increment(put_requests);
    }
}

void SpillFileManager::update_spill_remote_read(int64_t bytes, int64_t get_requests) {
    g_spill_remote_read_bytes << bytes;
    g_spill_remote_get_requests << get_requests;
    if (_spill_remote_read_bytes_counter != nullptr) {
        _spill_remote_read_bytes_counter->increment(bytes);
        _spill_remote_get_requests_counter->increment(get_requests);
    }
}

size_t SpillFileManager::pending_delete_dir_count() {
    std::lock_guard lock(_pending_query_spill_directories_mutex);
    return _pending_query_spill_directories.size();
}

// Retry failed query-directory deletions and clean up stale spill files.
void SpillFileManager::_spill_gc_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::spill_gc_interval_ms))) {
        gc(config::spill_gc_work_time_ms);
        for (auto& [path, dir] : _spill_store_map) {
            static_cast<void>(dir->update_capacity());
        }
        // Both configs are mutable; observe changes without a restart.
        int64_t budget_limit = config::spill_s3_max_inflight_upload_bytes;
        if (budget_limit < 2 * config::s3_write_buffer_size) {
            LOG_EVERY_T(WARNING, 60)
                    << "spill_s3_max_inflight_upload_bytes (" << budget_limit
                    << ") is below 2 * s3_write_buffer_size (" << config::s3_write_buffer_size
                    << "); spill uploads will run one buffer at a time";
        }
        if (budget_limit != _remote_upload_budget->limit_bytes()) {
            _remote_upload_budget->set_limit_bytes(budget_limit);
        }
        if (_spill_pending_delete_dir_count_gauge != nullptr) {
            _spill_pending_delete_dir_count_gauge->set_value(pending_delete_dir_count());
            _spill_remote_inflight_upload_bytes_gauge->set_value(
                    _remote_upload_budget->inflight_bytes());
            _spill_remote_startup_cleanup_pending_gauge->set_value(
                    remote_startup_cleanup_pending() ? 1 : 0);
        }
    }
}

Status SpillFileManager::_init_spill_store_map() {
    for (const auto& store : _spill_store_map) {
        RETURN_IF_ERROR(store.second->init());
    }

    return Status::OK();
}

std::vector<SpillDataDir*> SpillFileManager::_get_stores_for_spill(
        TStorageMedium::type storage_medium) {
    if (_remote_store != nullptr) {
        // Object storage has no medium; a remote store is the only store of the BE.
        if (_remote_store->reach_capacity_limit(0)) {
            return {};
        }
        return {_remote_store};
    }
    std::vector<std::pair<SpillDataDir*, double>> stores_with_usage;
    for (auto* store : _local_stores) {
        if (store->storage_medium() == storage_medium && !store->reach_capacity_limit(0)) {
            stores_with_usage.emplace_back(store, store->get_disk_usage(0));
        }
    }
    if (stores_with_usage.empty()) {
        return {};
    }

    std::ranges::sort(stores_with_usage, [](auto&& a, auto&& b) { return a.second < b.second; });

    std::vector<SpillDataDir*> stores;
    for (const auto& [store, _] : stores_with_usage) {
        stores.emplace_back(store);
    }
    return stores;
}

Status SpillFileManager::create_spill_file(const std::string& relative_path,
                                           SpillFileSPtr& spill_file) {
    if (_remote_store != nullptr) {
        RETURN_IF_ERROR(_remote_store->ensure_ready());
    }
    auto data_dirs = _get_stores_for_spill(TStorageMedium::type::SSD);
    if (data_dirs.empty()) {
        data_dirs = _get_stores_for_spill(TStorageMedium::type::HDD);
    }
    if (data_dirs.empty()) {
        return Status::Error<ErrorCode::NO_AVAILABLE_ROOT_PATH>(
                "no available disk can be used for spill.");
    }

    // Select the first available data dir (sorted by usage ascending)
    SpillDataDir* data_dir = data_dirs.front();
    spill_file = std::make_shared<SpillFile>(data_dir, relative_path);
    return Status::OK();
}

void SpillFileManager::delete_spill_file(SpillFileSPtr spill_file) {
    if (!spill_file) {
        LOG(WARNING) << "[spill][delete] null spill_file";
        return;
    }
    spill_file->gc();
}

void SpillFileManager::delete_query_spill_directory(const std::string& query_id,
                                                    SpillDataDir* data_dir) {
    PendingQuerySpillDirectory pending_directory {
            .query_dir = data_dir->get_spill_data_path(query_id),
            .data_dir = data_dir,
    };

    auto status = _try_delete_query_spill_directory(pending_directory);
    if (!status.ok()) {
        std::lock_guard lock(_pending_query_spill_directories_mutex);
        ++pending_directory.failed_count;
        _pending_query_spill_directories.emplace_back(std::move(pending_directory));
    }
}

Status SpillFileManager::_try_delete_query_spill_directory(
        const PendingQuerySpillDirectory& pending_directory) {
    DBUG_EXECUTE_IF("fault_inject::spill_file_manager::delete_query_spill_directory", {
        return Status::Error<INTERNAL_ERROR>("injected query spill directory deletion failure");
    });
    auto fs = pending_directory.data_dir != nullptr ? pending_directory.data_dir->fs()
                                                    : io::global_local_filesystem();
    if (fs == nullptr) {
        return Status::InternalError("spill store {} is not ready",
                                     pending_directory.data_dir->path());
    }
    return fs->delete_directory(pending_directory.query_dir);
}

void SpillFileManager::_retry_pending_query_spill_directories() {
    std::vector<PendingQuerySpillDirectory> pending_directories;
    {
        std::lock_guard lock(_pending_query_spill_directories_mutex);
        pending_directories.swap(_pending_query_spill_directories);
    }
    DBUG_EXECUTE_IF(
            "fault_inject::spill_file_manager::retry_pending_query_spill_directories_after_drain",
            { DBUG_RUN_CALLBACK(); });

    // Limit repeated warnings for a persistently unavailable directory while retaining it for
    // every subsequent retry.
    constexpr int log_interval = 5;
    std::vector<PendingQuerySpillDirectory> failed_directories;
    for (auto& pending_directory : pending_directories) {
        auto status = _try_delete_query_spill_directory(pending_directory);
        if (status.ok()) {
            continue;
        }

        ++pending_directory.failed_count;
        if (pending_directory.failed_count % log_interval == 0) {
            LOG(WARNING) << fmt::format(
                    "failed to retry deleting spill query directory, dir {}, error: {}",
                    pending_directory.query_dir, status.to_string());
        }
        failed_directories.emplace_back(std::move(pending_directory));
    }

    if (!failed_directories.empty()) {
        std::lock_guard lock(_pending_query_spill_directories_mutex);
        for (auto& pending_directory : failed_directories) {
            _pending_query_spill_directories.emplace_back(std::move(pending_directory));
        }
    }
}

void SpillFileManager::gc(int32_t max_work_time_ms) {
    bool exists = true;
    bool has_work = false;
    int64_t max_work_time_ns = max_work_time_ms * 1000L * 1000L;
    MonotonicStopWatch watch;
    watch.start();
    Defer defer {[&]() {
        if (has_work) {
            std::string msg(
                    fmt::format("spill gc time: {}",
                                PrettyPrinter::print(watch.elapsed_time(), TUnit::TIME_NS)));
            msg += ", spill storage:\n";
            for (const auto& [path, store_dir] : _spill_store_map) {
                msg += "    " + store_dir->debug_string();
                msg += "\n";
            }
            LOG(INFO) << msg;
        }
    }};
    _retry_pending_query_spill_directories();
    if (_remote_store != nullptr) {
        _remote_gc();
    }
    for (auto* store_dir : _local_stores) {
        std::string gc_root_dir = store_dir->get_spill_data_gc_path();

        std::error_code ec;
        exists = std::filesystem::exists(gc_root_dir, ec);
        if (ec || !exists) {
            continue;
        }
        // dirs of queries
        std::vector<io::FileInfo> dirs;
        auto st = io::global_local_filesystem()->list(gc_root_dir, false, &dirs, &exists);
        if (!st.ok()) {
            continue;
        }

        for (const auto& dir : dirs) {
            has_work = true;
            if (dir.is_file) {
                continue;
            }
            std::string abs_dir = fmt::format("{}/{}", gc_root_dir, dir.file_name);
            // operator spill sub dirs of a query
            std::vector<io::FileInfo> files;
            st = io::global_local_filesystem()->list(abs_dir, false, &files, &exists);
            if (!st.ok()) {
                continue;
            }
            if (files.empty()) {
                static_cast<void>(io::global_local_filesystem()->delete_directory(abs_dir));
                continue;
            }

            for (const auto& file : files) {
                auto abs_file_path = fmt::format("{}/{}", abs_dir, file.file_name);
                if (file.is_file) {
                    static_cast<void>(io::global_local_filesystem()->delete_file(abs_file_path));
                } else {
                    static_cast<void>(
                            io::global_local_filesystem()->delete_directory(abs_file_path));
                }
                if (watch.elapsed_time() > max_work_time_ns) {
                    break;
                }
            }
        }
    }
}

void SpillFileManager::_remote_gc() {
    if (!_remote_store->ready()) {
        // Retry about once a minute at the default 2s GC interval. ensure_ready() reads what
        // the vault refresh thread and the FE heartbeat already brought in, plus one bounded
        // meta-service call for the instance id.
        if (_remote_not_ready_rounds++ % 30 != 0) {
            return;
        }
        auto st = _remote_store->ensure_ready();
        if (!st.ok()) {
            LOG(WARNING) << "remote spill store is not ready yet: " << st;
            return;
        }
    }
    if (BackendOptions::get_backend_id() != _remote_store->backend_id()) {
        // DROP + ADD BACKEND of a live node hands it a new id. The store keeps the id it was
        // bound with (objects and stats of this process stay consistent); the new id takes
        // effect at the next restart, and the objects under the old id are left to the
        // meta-service recycler.
        LOG_EVERY_T(WARNING, 3600) << "backend id changed from " << _remote_store->backend_id()
                                   << " to " << BackendOptions::get_backend_id()
                                   << " while the remote spill store is bound; restart the BE to "
                                      "spill under the new id";
    }
    // Refresh the boot marker about once a day so that the recycler's age-based sweep never
    // removes the marker of a live process (see get_remote_boot_marker_path).
    const int64_t marker_refresh_rounds = std::max<int64_t>(
            1, 24LL * 3600 * 1000 / std::max<int32_t>(1, config::spill_gc_interval_ms));
    if (++_remote_boot_marker_rounds % marker_refresh_rounds == 0) {
        _remote_boot_marker_pending.store(true, std::memory_order_release);
    }
    if (_remote_boot_marker_pending.load(std::memory_order_acquire)) {
        auto st = _remote_write_boot_marker();
        if (!st.ok()) {
            LOG_EVERY_T(WARNING, 60) << "failed to write the spill boot marker, will retry: " << st;
            return;
        }
        _remote_boot_marker_pending.store(false, std::memory_order_release);
    }
    if (!remote_startup_cleanup_pending()) {
        return;
    }
    bool done = false;
    auto st = _remote_startup_cleanup(&done);
    if (!st.ok()) {
        LOG_EVERY_T(WARNING, 60) << "failed to clean up spill objects of previous boots, will "
                                    "retry: "
                                 << st;
    } else if (done) {
        _remote_startup_cleanup_pending.store(false, std::memory_order_release);
    }
}

Status SpillFileManager::_remote_write_boot_marker() {
    io::FileWriterPtr writer;
    RETURN_IF_ERROR(_remote_store->fs()->create_file(
            _remote_store->get_remote_boot_marker_path(std::to_string(_remote_store->boot_id())),
            &writer));
    return writer->close();
}

int64_t SpillFileManager::remote_spill_data_bytes() {
    return _remote_store != nullptr ? _remote_store->get_spill_data_bytes() : 0;
}

Status SpillFileManager::_remote_startup_cleanup(bool* done) {
    auto fs = _remote_store->fs();
    const auto current_boot_id = std::to_string(_remote_store->boot_id());

    MonotonicStopWatch watch;
    watch.start();
    // One marker object per boot generation of this BE; the listing is tiny even when a lot
    // of spill data is left behind. Only other generations are deleted; the current one is
    // being written by running queries.
    std::vector<io::FileInfo> markers;
    bool exists = false;
    RETURN_IF_ERROR(fs->list(_remote_store->get_remote_boots_path(), true, &markers, &exists));
    std::set<std::string> old_generations;
    for (const auto& marker : markers) {
        if (marker.is_file && !marker.file_name.empty() && marker.file_name != current_boot_id) {
            old_generations.emplace(marker.file_name);
        }
    }
    if (old_generations.empty()) {
        *done = true;
        return Status::OK();
    }
    // One generation per GC round keeps the GC thread responsive; the rest wait for the next
    // round. The marker goes last so that a failed data deletion is retried.
    const auto& generation = *old_generations.begin();
    RETURN_IF_ERROR(fs->delete_directory(_remote_store->get_remote_boot_data_path(generation)));
    RETURN_IF_ERROR(fs->delete_file(_remote_store->get_remote_boot_marker_path(generation)));
    *done = old_generations.size() == 1;
    LOG(INFO) << fmt::format(
            "cleaned up spill objects of a previous boot, be_root={}, current_boot_id={}, "
            "deleted_generation={}, remaining_generations={}, cost={}",
            _remote_store->get_remote_be_root(), current_boot_id, generation,
            old_generations.size() - 1, PrettyPrinter::print(watch.elapsed_time(), TUnit::TIME_NS));
    return Status::OK();
}

} // namespace doris
