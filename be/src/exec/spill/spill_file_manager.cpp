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

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "exec/spill/spill_file.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/query_context.h"
#include "storage/olap_define.h"
#include "util/debug_points.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/time.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

ExternalSpillSession::ExternalSpillSession(SpillFileManager* manager, QueryContext* query_context,
                                           std::string relative_path)
        : _manager(manager),
          _query_context(query_context->weak_from_this()),
          _resource_context(query_context->resource_ctx()),
          _query_id(print_id(query_context->query_id())),
          _relative_path(std::move(relative_path)) {
    DCHECK(_manager != nullptr);
    DCHECK(!_query_context.expired());
    DCHECK(_resource_context != nullptr);
}

ExternalSpillSession::~ExternalSpillSession() {
    _manager->_release_external_spill_session(this);
}

Status ExternalSpillSession::get_paths(std::vector<std::string>* paths) {
    if (paths == nullptr) {
        return Status::InvalidArgument("External spill paths output must not be null");
    }
    std::lock_guard lock(_mutex);
    if (_data_dir == nullptr) {
        RETURN_IF_ERROR(_manager->_initialize_external_spill_session(this));
    }
    *paths = {_path};
    return Status::OK();
}

bool ExternalSpillSession::_contains(const std::string& path) const {
    return path == _path ||
           (path.size() > _path.size() && path.starts_with(_path) && path[_path.size()] == '/');
}

Status ExternalSpillSession::reserve(const std::string& path, int64_t bytes) {
    if (bytes <= 0) {
        return Status::InvalidArgument("External spill reservation must be positive: {}", bytes);
    }

    std::lock_guard lock(_mutex);
    if (_data_dir == nullptr || !_contains(path)) {
        return Status::InvalidArgument("External spill path is not managed by Doris: {}", path);
    }
    if (bytes > std::numeric_limits<int64_t>::max() - _accounted_bytes) {
        return Status::InvalidArgument("External spill reservation overflows: bytes={}", bytes);
    }
    if (_data_dir->reach_capacity_limit(bytes)) {
        return Status::Error<ErrorCode::DISK_REACH_CAPACITY_LIMIT>(
                "External spill write exceeds the Doris spill storage limit: path={}, bytes={}",
                path, bytes);
    }
    // Match SpillFileWriter: check capacity before the write, then account the accepted bytes.
    _data_dir->update_spill_data_usage(bytes);
    _accounted_bytes += bytes;
    return Status::OK();
}

void ExternalSpillSession::update_accounting(const std::string& path, int64_t current_bytes_delta,
                                             int64_t write_bytes, int64_t read_bytes) {
    int64_t released_bytes = 0;
    SpillDataDir* data_dir = nullptr;
    {
        std::lock_guard lock(_mutex);
        if (_data_dir == nullptr || !_contains(path)) {
            LOG(WARNING) << "Ignoring accounting for unmanaged external spill path: " << path;
            return;
        }
        data_dir = _data_dir;
        if (current_bytes_delta < 0) {
            const int64_t requested_release =
                    current_bytes_delta == std::numeric_limits<int64_t>::min()
                            ? std::numeric_limits<int64_t>::max()
                            : -current_bytes_delta;
            released_bytes = std::min(requested_release, _accounted_bytes);
            _accounted_bytes -= released_bytes;
        }
    }
    if (released_bytes > 0) {
        data_dir->update_spill_data_usage(-released_bytes);
    }
    if (write_bytes > 0) {
        _resource_context->io_context()->update_spill_write_bytes_to_local_storage(write_bytes);
        _manager->update_spill_write_bytes(write_bytes);
    }
    if (read_bytes > 0) {
        _resource_context->io_context()->update_spill_read_bytes_from_local_storage(read_bytes);
        _manager->update_spill_read_bytes(read_bytes);
    }
}

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
        : _spill_store_map(std::move(spill_store_map)), _stop_background_threads_latch(1) {}

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

    for (const auto& [path, store] : _spill_store_map) {
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
}

// Retry failed query-directory deletions and clean up stale spill files.
void SpillFileManager::_spill_gc_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::spill_gc_interval_ms))) {
        gc(config::spill_gc_work_time_ms);
        for (auto& [path, dir] : _spill_store_map) {
            static_cast<void>(dir->update_capacity());
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
    std::vector<std::pair<SpillDataDir*, double>> stores_with_usage;
    for (auto& [_, store] : _spill_store_map) {
        if (store->storage_medium() == storage_medium && !store->reach_capacity_limit(0)) {
            stores_with_usage.emplace_back(store.get(), store->_get_disk_usage(0));
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

Status SpillFileManager::create_external_spill_session(
        const std::string& relative_path, QueryContext* query_context,
        std::unique_ptr<ExternalSpillSession>* spill_session) {
    if (query_context == nullptr || spill_session == nullptr) {
        return Status::InvalidArgument(
                "External spill session requires QueryContext and output session");
    }

    spill_session->reset(new ExternalSpillSession(this, query_context, relative_path));
    return Status::OK();
}

Status SpillFileManager::_initialize_external_spill_session(ExternalSpillSession* spill_session) {
    auto query_context = spill_session->_query_context.lock();
    if (query_context == nullptr) {
        return Status::Cancelled("Query ended before the external spill session was initialized");
    }
    auto* data_dir = _get_store_for_spill();
    if (data_dir == nullptr) {
        return Status::Error<ErrorCode::NO_AVAILABLE_ROOT_PATH>(
                "no available disk can be used for spill.");
    }

    const auto query_dir = data_dir->get_spill_data_path(spill_session->_query_id);
    {
        // QueryContext teardown uses the regular pending-deletion path while this lease is live.
        std::lock_guard lock(_pending_query_spill_directories_mutex);
        ++_external_spill_directory_leases[query_dir];
    }
    query_context->record_spill_data_dir(data_dir);
    spill_session->_data_dir = data_dir;
    spill_session->_path = query_dir + "/" + spill_session->_relative_path;
    return Status::OK();
}

void SpillFileManager::_release_external_spill_session(ExternalSpillSession* spill_session) {
    std::lock_guard session_lock(spill_session->_mutex);
    if (spill_session->_data_dir == nullptr) {
        return;
    }

    if (spill_session->_accounted_bytes > 0) {
        // Match SpillFile::gc(): QueryContext owns physical cleanup and its retry path, while the
        // writer releases logical usage when its lifetime ends.
        spill_session->_data_dir->update_spill_data_usage(-spill_session->_accounted_bytes);
        spill_session->_accounted_bytes = 0;
    }

    const auto query_dir = spill_session->_data_dir->get_spill_data_path(spill_session->_query_id);
    std::lock_guard directory_lock(_pending_query_spill_directories_mutex);
    auto it = _external_spill_directory_leases.find(query_dir);
    DCHECK(it != _external_spill_directory_leases.end());
    if (it == _external_spill_directory_leases.end()) {
        return;
    }
    DCHECK_GT(it->second, 0);
    if (--it->second == 0) {
        _external_spill_directory_leases.erase(it);
    }
}

SpillDataDir* SpillFileManager::_get_store_for_spill() {
    auto data_dirs = _get_stores_for_spill(TStorageMedium::type::SSD);
    if (data_dirs.empty()) {
        data_dirs = _get_stores_for_spill(TStorageMedium::type::HDD);
    }
    // Select the first available data dir (sorted by usage ascending).
    return data_dirs.empty() ? nullptr : data_dirs.front();
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
    {
        std::lock_guard lock(_pending_query_spill_directories_mutex);
        if (_external_spill_directory_leases.contains(pending_directory.query_dir)) {
            return Status::InternalError("external spill directory is still in use: {}",
                                         pending_directory.query_dir);
        }
    }
    DBUG_EXECUTE_IF("fault_inject::spill_file_manager::delete_query_spill_directory", {
        return Status::Error<INTERNAL_ERROR>("injected query spill directory deletion failure");
    });
    const auto& fs = io::global_local_filesystem();
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
    for (const auto& [path, store_dir] : _spill_store_map) {
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

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_limit, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_avail_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_data_size, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_has_spill_data, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(spill_disk_has_spill_gc_data, MetricUnit::BYTES);

SpillDataDir::SpillDataDir(std::string path, int64_t capacity_bytes,
                           TStorageMedium::type storage_medium)
        : _path(std::move(path)),
          _disk_capacity_bytes(capacity_bytes),
          _storage_medium(storage_medium) {
    spill_data_dir_metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("spill_data_dir.") + _path, {{"path", _path + "/" + SPILL_DIR_PREFIX}});
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_capacity);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_limit);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_avail_capacity);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_data_size);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_has_spill_data);
    INT_GAUGE_METRIC_REGISTER(spill_data_dir_metric_entity, spill_disk_has_spill_gc_data);
}

bool is_directory_empty(const std::filesystem::path& dir) {
    // Spill cleanup may delete the directory while the iterator is constructed or advanced. Treat
    // that race as empty for these presence metrics.
    try {
        return std::filesystem::is_directory(dir) &&
               std::filesystem::directory_iterator(dir) ==
                       std::filesystem::end(std::filesystem::directory_iterator {});
    } catch (const std::filesystem::filesystem_error&) {
        return true;
    }
}

Status SpillDataDir::init() {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(_path, &exists));
    if (!exists) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError("opendir failed, path={}", _path),
                                       "check file exist failed");
    }
    RETURN_IF_ERROR(update_capacity());
    LOG(INFO) << fmt::format(
            "spill storage path: {}, capacity: {}, limit: {}, available: "
            "{}",
            _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
            PrettyPrinter::print_bytes(_spill_data_limit_bytes),
            PrettyPrinter::print_bytes(_available_bytes));
    return Status::OK();
}

std::string SpillDataDir::get_spill_data_path(const std::string& query_id) const {
    auto dir = fmt::format("{}/{}", _path, SPILL_DIR_PREFIX);
    if (!query_id.empty()) {
        dir = fmt::format("{}/{}", dir, query_id);
    }
    return dir;
}

std::string SpillDataDir::get_spill_data_gc_path(const std::string& sub_dir_name) const {
    auto dir = fmt::format("{}/{}", _path, SPILL_GC_DIR_PREFIX);
    if (!sub_dir_name.empty()) {
        dir = fmt::format("{}/{}", dir, sub_dir_name);
    }
    return dir;
}

Status SpillDataDir::update_capacity() {
    std::lock_guard<std::mutex> l(_mutex);
    RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(_path, &_disk_capacity_bytes,
                                                                  &_available_bytes));
    spill_disk_capacity->set_value(_disk_capacity_bytes);
    spill_disk_avail_capacity->set_value(_available_bytes);
    auto disk_use_max_bytes =
            (int64_t)(_disk_capacity_bytes * config::storage_flood_stage_usage_percent / 100);
    bool is_percent = true;
    _spill_data_limit_bytes = ParseUtil::parse_mem_spec(config::spill_storage_limit, -1,
                                                        _disk_capacity_bytes, &is_percent);
    if (_spill_data_limit_bytes <= 0) {
        spill_disk_limit->set_value(_spill_data_limit_bytes);
        auto err_msg = fmt::format("Failed to parse spill storage limit from '{}'",
                                   config::spill_storage_limit);
        LOG(WARNING) << err_msg;
        return Status::InvalidArgument(err_msg);
    }
    if (is_percent) {
        _spill_data_limit_bytes = (int64_t)(_spill_data_limit_bytes *
                                            config::storage_flood_stage_usage_percent / 100);
    }
    _spill_data_limit_bytes = std::min(_spill_data_limit_bytes, disk_use_max_bytes);
    spill_disk_limit->set_value(_spill_data_limit_bytes);

    std::string spill_root_dir = get_spill_data_path();
    std::string spill_gc_root_dir = get_spill_data_gc_path();
    spill_disk_has_spill_data->set_value(is_directory_empty(spill_root_dir) ? 0 : 1);
    spill_disk_has_spill_gc_data->set_value(is_directory_empty(spill_gc_root_dir) ? 0 : 1);

    return Status::OK();
}

bool SpillDataDir::_reach_disk_capacity_limit(int64_t incoming_data_size) {
    double used_pct = _get_disk_usage(incoming_data_size);
    int64_t left_bytes = _available_bytes - incoming_data_size;
    if (used_pct >= config::storage_flood_stage_usage_percent / 100.0 &&
        left_bytes <= config::storage_flood_stage_left_capacity_bytes) {
        LOG(WARNING) << "reach capacity limit. used pct: " << used_pct
                     << ", left bytes: " << left_bytes << ", path: " << _path;
        return true;
    }
    return false;
}
bool SpillDataDir::reach_capacity_limit(int64_t incoming_data_size) {
    std::lock_guard<std::mutex> l(_mutex);
    if (_reach_disk_capacity_limit(incoming_data_size)) {
        return true;
    }
    if (_spill_data_bytes + incoming_data_size > _spill_data_limit_bytes) {
        LOG_EVERY_T(WARNING, 1) << fmt::format(
                "spill data reach limit, path: {}, capacity: {}, limit: {}, used: {}, "
                "available: "
                "{}, "
                "incoming "
                "bytes: {}",
                _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
                PrettyPrinter::print_bytes(_spill_data_limit_bytes),
                PrettyPrinter::print_bytes(_spill_data_bytes),
                PrettyPrinter::print_bytes(_available_bytes),
                PrettyPrinter::print_bytes(incoming_data_size));
        return true;
    }
    return false;
}
std::string SpillDataDir::debug_string() {
    return fmt::format(
            "path: {}, capacity: {}, limit: {}, used: {}, available: "
            "{}",
            _path, PrettyPrinter::print_bytes(_disk_capacity_bytes),
            PrettyPrinter::print_bytes(_spill_data_limit_bytes),
            PrettyPrinter::print_bytes(_spill_data_bytes),
            PrettyPrinter::print_bytes(_available_bytes));
}
} // namespace doris
