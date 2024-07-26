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

#include "cloud/cloud_storage_engine.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/cloud.pb.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <variant>

#include "cloud/cloud_base_compaction.h"
#include "cloud/cloud_cumulative_compaction.h"
#include "cloud/cloud_cumulative_compaction_policy.h"
#include "cloud/cloud_full_compaction.h"
#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/cloud_txn_delete_bitmap_cache.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/config.h"
#include "io/cache/block_file_cache_downloader.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_util.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/memtable_flush_executor.h"
#include "olap/storage_policy.h"
#include "runtime/memory/cache_manager.h"

namespace doris {

using namespace std::literals;

int get_cumu_thread_num() {
    if (config::max_cumu_compaction_threads > 0) {
        return config::max_cumu_compaction_threads;
    }

    int num_cores = doris::CpuInfo::num_cores();
    return std::min(std::max(int(num_cores * config::cumu_compaction_thread_num_factor), 2), 20);
}

int get_base_thread_num() {
    if (config::max_base_compaction_threads > 0) {
        return config::max_base_compaction_threads;
    }

    int num_cores = doris::CpuInfo::num_cores();
    return std::min(std::max(int(num_cores * config::base_compaction_thread_num_factor), 1), 10);
}

CloudStorageEngine::CloudStorageEngine(const UniqueId& backend_uid)
        : BaseStorageEngine(Type::CLOUD, backend_uid),
          _meta_mgr(std::make_unique<cloud::CloudMetaMgr>()),
          _tablet_mgr(std::make_unique<CloudTabletMgr>(*this)) {
    _cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            std::make_shared<CloudSizeBasedCumulativeCompactionPolicy>();
    _cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            std::make_shared<CloudTimeSeriesCumulativeCompactionPolicy>();
}

CloudStorageEngine::~CloudStorageEngine() {
    stop();
}

static Status vault_process_error(std::string_view id,
                                  std::variant<S3Conf, cloud::HdfsVaultInfo>& vault, Status err) {
    std::stringstream ss;
    std::visit(
            [&]<typename T>(T& val) {
                if constexpr (std::is_same_v<T, S3Conf>) {
                    ss << val.to_string();
                } else if constexpr (std::is_same_v<T, cloud::HdfsVaultInfo>) {
                    val.SerializeToOstream(&ss);
                }
            },
            vault);
    return Status::IOError("Invalid vault, id {}, err {}, detail conf {}", id, err, ss.str());
}

struct VaultCreateFSVisitor {
    VaultCreateFSVisitor(const std::string& id, const cloud::StorageVaultPB_PathFormat& path_format)
            : id(id), path_format(path_format) {}
    Status operator()(const S3Conf& s3_conf) const {
        LOG(INFO) << "get new s3 info: " << s3_conf.to_string() << " resource_id=" << id;

        auto fs = DORIS_TRY(io::S3FileSystem::create(s3_conf, id));
        put_storage_resource(id, {std::move(fs), path_format}, 0);
        LOG_INFO("successfully create s3 vault, vault id {}", id);
        return Status::OK();
    }

    // TODO(ByteYue): Make sure enable_java_support is on
    Status operator()(const cloud::HdfsVaultInfo& vault) const {
        auto hdfs_params = io::to_hdfs_params(vault);
        auto fs = DORIS_TRY(io::HdfsFileSystem::create(hdfs_params, hdfs_params.fs_name, id,
                                                       nullptr, vault.prefix()));
        put_storage_resource(id, {std::move(fs), path_format}, 0);
        LOG_INFO("successfully create hdfs vault, vault id {}", id);
        return Status::OK();
    }

    const std::string& id;
    const cloud::StorageVaultPB_PathFormat& path_format;
};

struct RefreshFSVaultVisitor {
    RefreshFSVaultVisitor(const std::string& id, io::FileSystemSPtr fs,
                          const cloud::StorageVaultPB_PathFormat& path_format)
            : id(id), fs(std::move(fs)), path_format(path_format) {}

    Status operator()(const S3Conf& s3_conf) const {
        DCHECK_EQ(fs->type(), io::FileSystemType::S3) << id;
        auto s3_fs = std::static_pointer_cast<io::S3FileSystem>(fs);
        auto client_holder = s3_fs->client_holder();
        auto st = client_holder->reset(s3_conf.client_conf);
        if (!st.ok()) {
            LOG(WARNING) << "failed to update s3 fs, resource_id=" << id << ": " << st;
        }
        return st;
    }

    Status operator()(const cloud::HdfsVaultInfo& vault) const {
        auto hdfs_params = io::to_hdfs_params(vault);
        auto hdfs_fs =
                DORIS_TRY(io::HdfsFileSystem::create(hdfs_params, hdfs_params.fs_name, id, nullptr,
                                                     vault.has_prefix() ? vault.prefix() : ""));
        auto hdfs = std::static_pointer_cast<io::HdfsFileSystem>(hdfs_fs);
        put_storage_resource(id, {std::move(hdfs), path_format}, 0);
        return Status::OK();
    }

    const std::string& id;
    io::FileSystemSPtr fs;
    const cloud::StorageVaultPB_PathFormat& path_format;
};

Status CloudStorageEngine::open() {
    cloud::StorageVaultInfos vault_infos;
    do {
        auto st = _meta_mgr->get_storage_vault_info(&vault_infos);
        if (st.ok()) {
            break;
        }

        LOG(WARNING) << "failed to get vault info, retry after 5s, err=" << st;
        std::this_thread::sleep_for(5s);
    } while (vault_infos.empty());

    for (auto& [id, vault_info, path_format] : vault_infos) {
        if (auto st = std::visit(VaultCreateFSVisitor {id, path_format}, vault_info); !st.ok())
                [[unlikely]] {
            return vault_process_error(id, vault_info, std::move(st));
        }
    }
    set_latest_fs(get_filesystem(std::get<0>(vault_infos.back())));

    // TODO(plat1ko): DeleteBitmapTxnManager

    _memtable_flush_executor = std::make_unique<MemTableFlushExecutor>();
    // TODO(plat1ko): Use file cache disks number?
    _memtable_flush_executor->init(1);

    _calc_delete_bitmap_executor = std::make_unique<CalcDeleteBitmapExecutor>();
    _calc_delete_bitmap_executor->init();

    _txn_delete_bitmap_cache =
            std::make_unique<CloudTxnDeleteBitmapCache>(config::delete_bitmap_agg_cache_capacity);
    RETURN_IF_ERROR(_txn_delete_bitmap_cache->init());

    _file_cache_block_downloader = std::make_unique<io::FileCacheBlockDownloader>(*this);

    _cloud_warm_up_manager = std::make_unique<CloudWarmUpManager>(*this);

    _tablet_hotspot = std::make_unique<TabletHotspot>();

    RETURN_NOT_OK_STATUS_WITH_WARN(
            init_stream_load_recorder(ExecEnv::GetInstance()->store_paths()[0].path),
            "init StreamLoadRecorder failed");

    return ThreadPoolBuilder("SyncLoadForTabletsThreadPool")
            .set_max_threads(config::sync_load_for_tablets_thread)
            .set_min_threads(config::sync_load_for_tablets_thread)
            .build(&_sync_load_for_tablets_thread_pool);
}

void CloudStorageEngine::stop() {
    if (_stopped) {
        return;
    }

    _stopped = true;
    _stop_background_threads_latch.count_down();

    for (auto&& t : _bg_threads) {
        if (t) {
            t->join();
        }
    }
}

bool CloudStorageEngine::stopped() {
    return _stopped;
}

Result<BaseTabletSPtr> CloudStorageEngine::get_tablet(int64_t tablet_id) {
    return _tablet_mgr->get_tablet(tablet_id, false).transform([](auto&& t) {
        return static_pointer_cast<BaseTablet>(std::move(t));
    });
}

Status CloudStorageEngine::start_bg_threads() {
    RETURN_IF_ERROR(Thread::create(
            "CloudStorageEngine", "refresh_s3_info_thread",
            [this]() { this->_refresh_storage_vault_info_thread_callback(); },
            &_bg_threads.emplace_back()));
    LOG(INFO) << "refresh s3 info thread started";

    RETURN_IF_ERROR(Thread::create(
            "CloudStorageEngine", "vacuum_stale_rowsets_thread",
            [this]() { this->_vacuum_stale_rowsets_thread_callback(); },
            &_bg_threads.emplace_back()));
    LOG(INFO) << "vacuum stale rowsets thread started";

    RETURN_IF_ERROR(Thread::create(
            "CloudStorageEngine", "sync_tablets_thread",
            [this]() { this->_sync_tablets_thread_callback(); }, &_bg_threads.emplace_back()));
    LOG(INFO) << "sync tablets thread started";

    RETURN_IF_ERROR(Thread::create(
            "CloudStorageEngine", "evict_querying_rowset_thread",
            [this]() { this->_evict_quring_rowset_thread_callback(); },
            &_evict_quering_rowset_thread));
    LOG(INFO) << "evict quering thread started";

    // add calculate tablet delete bitmap task thread pool
    RETURN_IF_ERROR(ThreadPoolBuilder("TabletCalDeleteBitmapThreadPool")
                            .set_min_threads(1)
                            .set_max_threads(config::calc_tablet_delete_bitmap_task_max_thread)
                            .build(&_calc_tablet_delete_bitmap_task_thread_pool));

    // TODO(plat1ko): check_bucket_enable_versioning_thread

    // compaction tasks producer thread
    int base_thread_num = get_base_thread_num();
    int cumu_thread_num = get_cumu_thread_num();
    RETURN_IF_ERROR(ThreadPoolBuilder("BaseCompactionTaskThreadPool")
                            .set_min_threads(base_thread_num)
                            .set_max_threads(base_thread_num)
                            .build(&_base_compaction_thread_pool));
    RETURN_IF_ERROR(ThreadPoolBuilder("CumuCompactionTaskThreadPool")
                            .set_min_threads(cumu_thread_num)
                            .set_max_threads(cumu_thread_num)
                            .build(&_cumu_compaction_thread_pool));
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "compaction_tasks_producer_thread",
            [this]() { this->_compaction_tasks_producer_callback(); },
            &_bg_threads.emplace_back()));
    LOG(INFO) << "compaction tasks producer thread started,"
              << " base thread num " << base_thread_num << " cumu thread num " << cumu_thread_num;

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "lease_compaction_thread",
            [this]() { this->_lease_compaction_thread_callback(); }, &_bg_threads.emplace_back()));

    if (config::file_cache_ttl_valid_check_interval_second != 0) {
        RETURN_IF_ERROR(Thread::create(
                "StorageEngine", "check_file_cache_ttl_block_valid_thread",
                [this]() { this->_check_file_cache_ttl_block_valid(); },
                &_bg_threads.emplace_back()));
        LOG(INFO) << "check file cache ttl block valid thread started";
    }

    LOG(INFO) << "lease compaction thread started";

    return Status::OK();
}

void CloudStorageEngine::_check_file_cache_ttl_block_valid() {
    int64_t interval_seconds = config::file_cache_ttl_valid_check_interval_second / 2;
    auto check_ttl = [](const std::weak_ptr<CloudTablet>& tablet_wk) {
        auto tablet = tablet_wk.lock();
        if (!tablet) return;
        if (tablet->tablet_meta()->ttl_seconds() == 0) return;
        auto rowsets = tablet->get_snapshot_rowset();
        for (const auto& rowset : rowsets) {
            int64_t ttl_seconds = tablet->tablet_meta()->ttl_seconds();
            if (rowset->newest_write_timestamp() + ttl_seconds <= UnixSeconds()) continue;
            for (int64_t seg_id = 0; seg_id < rowset->num_segments(); seg_id++) {
                auto hash = Segment::file_cache_key(rowset->rowset_id().to_string(), seg_id);
                auto* file_cache = io::FileCacheFactory::instance()->get_by_path(hash);
                file_cache->update_ttl_atime(hash);
            }
        }
    };
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval_seconds))) {
        auto weak_tablets = tablet_mgr().get_weak_tablets();
        std::for_each(weak_tablets.begin(), weak_tablets.end(), check_ttl);
    }
}

void CloudStorageEngine::sync_storage_vault() {
    cloud::StorageVaultInfos vault_infos;
    auto st = _meta_mgr->get_storage_vault_info(&vault_infos);
    if (!st.ok()) {
        LOG(WARNING) << "failed to get storage vault info. err=" << st;
        return;
    }

    if (vault_infos.empty()) {
        LOG(WARNING) << "empty storage vault info";
        return;
    }

    for (auto& [id, vault_info, path_format] : vault_infos) {
        auto fs = get_filesystem(id);
        auto st = (fs == nullptr)
                          ? std::visit(VaultCreateFSVisitor {id, path_format}, vault_info)
                          : std::visit(RefreshFSVaultVisitor {id, std::move(fs), path_format},
                                       vault_info);
        if (!st.ok()) [[unlikely]] {
            LOG(WARNING) << vault_process_error(id, vault_info, std::move(st));
        }
    }

    if (auto& id = std::get<0>(vault_infos.back());
        latest_fs() == nullptr || latest_fs()->id() != id) {
        set_latest_fs(get_filesystem(id));
    }
}

// We should enable_java_support if we want to use hdfs vault
void CloudStorageEngine::_refresh_storage_vault_info_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::refresh_s3_info_interval_s))) {
        sync_storage_vault();
    }
}

void CloudStorageEngine::_vacuum_stale_rowsets_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::vacuum_stale_rowsets_interval_s))) {
        _tablet_mgr->vacuum_stale_rowsets(_stop_background_threads_latch);
    }
}

void CloudStorageEngine::_sync_tablets_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::schedule_sync_tablets_interval_s))) {
        _tablet_mgr->sync_tablets(_stop_background_threads_latch);
    }
}

void CloudStorageEngine::get_cumu_compaction(
        int64_t tablet_id, std::vector<std::shared_ptr<CloudCumulativeCompaction>>& res) {
    std::lock_guard lock(_compaction_mtx);
    if (auto it = _submitted_cumu_compactions.find(tablet_id);
        it != _submitted_cumu_compactions.end()) {
        res = it->second;
    }
}

Status CloudStorageEngine::_adjust_compaction_thread_num() {
    int base_thread_num = get_base_thread_num();

    if (!_base_compaction_thread_pool || !_cumu_compaction_thread_pool) {
        LOG(WARNING) << "base or cumu compaction thread pool is not created";
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("");
    }

    if (_base_compaction_thread_pool->max_threads() != base_thread_num) {
        int old_max_threads = _base_compaction_thread_pool->max_threads();
        Status status = _base_compaction_thread_pool->set_max_threads(base_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update base compaction thread pool max_threads from " << old_max_threads
                        << " to " << base_thread_num;
        }
    }
    if (_base_compaction_thread_pool->min_threads() != base_thread_num) {
        int old_min_threads = _base_compaction_thread_pool->min_threads();
        Status status = _base_compaction_thread_pool->set_min_threads(base_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update base compaction thread pool min_threads from " << old_min_threads
                        << " to " << base_thread_num;
        }
    }

    int cumu_thread_num = get_cumu_thread_num();
    if (_cumu_compaction_thread_pool->max_threads() != cumu_thread_num) {
        int old_max_threads = _cumu_compaction_thread_pool->max_threads();
        Status status = _cumu_compaction_thread_pool->set_max_threads(cumu_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update cumu compaction thread pool max_threads from " << old_max_threads
                        << " to " << cumu_thread_num;
        }
    }
    if (_cumu_compaction_thread_pool->min_threads() != cumu_thread_num) {
        int old_min_threads = _cumu_compaction_thread_pool->min_threads();
        Status status = _cumu_compaction_thread_pool->set_min_threads(cumu_thread_num);
        if (status.ok()) {
            VLOG_NOTICE << "update cumu compaction thread pool min_threads from " << old_min_threads
                        << " to " << cumu_thread_num;
        }
    }
    return Status::OK();
}

void CloudStorageEngine::_compaction_tasks_producer_callback() {
    LOG(INFO) << "try to start compaction producer process!";

    int round = 0;
    CompactionType compaction_type;

    // Used to record the time when the score metric was last updated.
    // The update of the score metric is accompanied by the logic of selecting the tablet.
    // If there is no slot available, the logic of selecting the tablet will be terminated,
    // which causes the score metric update to be terminated.
    // In order to avoid this situation, we need to update the score regularly.
    int64_t last_cumulative_score_update_time = 0;
    int64_t last_base_score_update_time = 0;
    static const int64_t check_score_interval_ms = 5000; // 5 secs

    int64_t interval = config::generate_compaction_tasks_interval_ms;
    do {
        if (!config::disable_auto_compaction) {
            Status st = _adjust_compaction_thread_num();
            if (!st.ok()) {
                break;
            }

            bool check_score = false;
            int64_t cur_time = UnixMillis();
            if (round < config::cumulative_compaction_rounds_for_each_base_compaction_round) {
                compaction_type = CompactionType::CUMULATIVE_COMPACTION;
                round++;
                if (cur_time - last_cumulative_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_cumulative_score_update_time = cur_time;
                }
            } else {
                compaction_type = CompactionType::BASE_COMPACTION;
                round = 0;
                if (cur_time - last_base_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_base_score_update_time = cur_time;
                }
            }
            std::unique_ptr<ThreadPool>& thread_pool =
                    (compaction_type == CompactionType::CUMULATIVE_COMPACTION)
                            ? _cumu_compaction_thread_pool
                            : _base_compaction_thread_pool;
            VLOG_CRITICAL << "compaction thread pool. type: "
                          << (compaction_type == CompactionType::CUMULATIVE_COMPACTION ? "CUMU"
                                                                                       : "BASE")
                          << ", num_threads: " << thread_pool->num_threads()
                          << ", num_threads_pending_start: "
                          << thread_pool->num_threads_pending_start()
                          << ", num_active_threads: " << thread_pool->num_active_threads()
                          << ", max_threads: " << thread_pool->max_threads()
                          << ", min_threads: " << thread_pool->min_threads()
                          << ", num_total_queued_tasks: " << thread_pool->get_queue_size();
            std::vector<CloudTabletSPtr> tablets_compaction =
                    _generate_cloud_compaction_tasks(compaction_type, check_score);

            /// Regardless of whether the tablet is submitted for compaction or not,
            /// we need to call 'reset_compaction' to clean up the base_compaction or cumulative_compaction objects
            /// in the tablet, because these two objects store the tablet's own shared_ptr.
            /// If it is not cleaned up, the reference count of the tablet will always be greater than 1,
            /// thus cannot be collected by the garbage collector. (TabletManager::start_trash_sweep)
            for (const auto& tablet : tablets_compaction) {
                Status st = submit_compaction_task(tablet, compaction_type);
                if (st.ok()) continue;
                if ((!st.is<ErrorCode::BE_NO_SUITABLE_VERSION>() &&
                     !st.is<ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION>()) ||
                    VLOG_DEBUG_IS_ON) {
                    LOG(WARNING) << "failed to submit compaction task for tablet: "
                                 << tablet->tablet_id() << ", err: " << st;
                }
            }
            interval = config::generate_compaction_tasks_interval_ms;
        } else {
            interval = config::check_auto_compaction_interval_seconds * 1000;
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(interval)));
}

std::vector<CloudTabletSPtr> CloudStorageEngine::_generate_cloud_compaction_tasks(
        CompactionType compaction_type, bool check_score) {
    std::vector<std::shared_ptr<CloudTablet>> tablets_compaction;

    int64_t max_compaction_score = 0;
    std::unordered_set<int64_t> tablet_preparing_cumu_compaction;
    std::unordered_map<int64_t, std::vector<std::shared_ptr<CloudCumulativeCompaction>>>
            submitted_cumu_compactions;
    std::unordered_map<int64_t, std::shared_ptr<CloudBaseCompaction>> submitted_base_compactions;
    std::unordered_map<int64_t, std::shared_ptr<CloudFullCompaction>> submitted_full_compactions;
    {
        std::lock_guard lock(_compaction_mtx);
        tablet_preparing_cumu_compaction = _tablet_preparing_cumu_compaction;
        submitted_cumu_compactions = _submitted_cumu_compactions;
        submitted_base_compactions = _submitted_base_compactions;
        submitted_full_compactions = _submitted_full_compactions;
    }

    bool need_pick_tablet = true;
    int thread_per_disk =
            config::compaction_task_num_per_fast_disk; // all disks are fast in cloud mode
    int num_cumu =
            std::accumulate(submitted_cumu_compactions.begin(), submitted_cumu_compactions.end(), 0,
                            [](int a, auto& b) { return a + b.second.size(); });
    int num_base = submitted_base_compactions.size() + submitted_full_compactions.size();
    int n = thread_per_disk - num_cumu - num_base;
    if (compaction_type == CompactionType::BASE_COMPACTION) {
        // We need to reserve at least one thread for cumulative compaction,
        // because base compactions may take too long to complete, which may
        // leads to "too many rowsets" error.
        int base_n = std::min(config::max_base_compaction_task_num_per_disk, thread_per_disk - 1) -
                     num_base;
        n = std::min(base_n, n);
    }
    if (n <= 0) { // No threads available
        if (!check_score) return tablets_compaction;
        need_pick_tablet = false;
        n = 0;
    }

    // Return true for skipping compaction
    std::function<bool(CloudTablet*)> filter_out;
    if (compaction_type == CompactionType::BASE_COMPACTION) {
        filter_out = [&submitted_base_compactions, &submitted_full_compactions](CloudTablet* t) {
            return !!submitted_base_compactions.count(t->tablet_id()) ||
                   !!submitted_full_compactions.count(t->tablet_id()) ||
                   t->tablet_state() != TABLET_RUNNING;
        };
    } else if (config::enable_parallel_cumu_compaction) {
        filter_out = [&tablet_preparing_cumu_compaction](CloudTablet* t) {
            return !!tablet_preparing_cumu_compaction.count(t->tablet_id()) ||
                   t->tablet_state() != TABLET_RUNNING;
        };
    } else {
        filter_out = [&tablet_preparing_cumu_compaction,
                      &submitted_cumu_compactions](CloudTablet* t) {
            return !!tablet_preparing_cumu_compaction.count(t->tablet_id()) ||
                   !!submitted_cumu_compactions.count(t->tablet_id()) ||
                   t->tablet_state() != TABLET_RUNNING;
        };
    }

    // Even if need_pick_tablet is false, we still need to call find_best_tablet_to_compaction(),
    // So that we can update the max_compaction_score metric.
    do {
        std::vector<CloudTabletSPtr> tablets;
        auto st = tablet_mgr().get_topn_tablets_to_compact(n, compaction_type, filter_out, &tablets,
                                                           &max_compaction_score);
        if (!st.ok()) {
            LOG(WARNING) << "failed to get tablets to compact, err=" << st;
            break;
        }
        if (!need_pick_tablet) break;
        tablets_compaction = std::move(tablets);
    } while (false);

    if (max_compaction_score > 0) {
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            DorisMetrics::instance()->tablet_base_max_compaction_score->set_value(
                    max_compaction_score);
        } else {
            DorisMetrics::instance()->tablet_cumulative_max_compaction_score->set_value(
                    max_compaction_score);
        }
    }

    return tablets_compaction;
}

Status CloudStorageEngine::_submit_base_compaction_task(const CloudTabletSPtr& tablet) {
    using namespace std::chrono;
    {
        std::lock_guard lock(_compaction_mtx);
        // Take a placeholder for base compaction
        auto [_, success] = _submitted_base_compactions.emplace(tablet->tablet_id(), nullptr);
        if (!success) {
            return Status::AlreadyExist(
                    "other base compaction or full compaction is submitted, tablet_id={}",
                    tablet->tablet_id());
        }
    }
    auto compaction = std::make_shared<CloudBaseCompaction>(*this, tablet);
    auto st = compaction->prepare_compact();
    if (!st.ok()) {
        long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        tablet->set_last_base_compaction_failure_time(now);
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions.erase(tablet->tablet_id());
        return st;
    }
    {
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions[tablet->tablet_id()] = compaction;
    }
    st = _base_compaction_thread_pool->submit_func([=, this, compaction = std::move(compaction)]() {
        auto st = compaction->execute_compact();
        if (!st.ok()) {
            // Error log has been output in `execute_compact`
            long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            tablet->set_last_base_compaction_failure_time(now);
        }
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions.erase(tablet->tablet_id());
    });
    if (!st.ok()) {
        std::lock_guard lock(_compaction_mtx);
        _submitted_base_compactions.erase(tablet->tablet_id());
        return Status::InternalError("failed to submit base compaction, tablet_id={}",
                                     tablet->tablet_id());
    }
    return st;
}

Status CloudStorageEngine::_submit_cumulative_compaction_task(const CloudTabletSPtr& tablet) {
    using namespace std::chrono;
    {
        std::lock_guard lock(_compaction_mtx);
        if (!config::enable_parallel_cumu_compaction &&
            _submitted_cumu_compactions.count(tablet->tablet_id())) {
            return Status::AlreadyExist("other cumu compaction is submitted, tablet_id={}",
                                        tablet->tablet_id());
        }
        auto [_, success] = _tablet_preparing_cumu_compaction.insert(tablet->tablet_id());
        if (!success) {
            return Status::AlreadyExist("other cumu compaction is preparing, tablet_id={}",
                                        tablet->tablet_id());
        }
    }
    auto compaction = std::make_shared<CloudCumulativeCompaction>(*this, tablet);
    auto st = compaction->prepare_compact();
    if (!st.ok()) {
        long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        if (st.is<ErrorCode::CUMULATIVE_NO_SUITABLE_VERSION>()) {
            // Backoff strategy if no suitable version
            tablet->last_cumu_no_suitable_version_ms = now;
        }
        tablet->set_last_cumu_compaction_failure_time(now);
        std::lock_guard lock(_compaction_mtx);
        _tablet_preparing_cumu_compaction.erase(tablet->tablet_id());
        return st;
    }
    {
        std::lock_guard lock(_compaction_mtx);
        _tablet_preparing_cumu_compaction.erase(tablet->tablet_id());
        _submitted_cumu_compactions[tablet->tablet_id()].push_back(compaction);
    }
    auto erase_submitted_cumu_compaction = [=, this]() {
        std::lock_guard lock(_compaction_mtx);
        auto it = _submitted_cumu_compactions.find(tablet->tablet_id());
        DCHECK(it != _submitted_cumu_compactions.end());
        auto& compactions = it->second;
        auto it1 = std::find(compactions.begin(), compactions.end(), compaction);
        DCHECK(it1 != compactions.end());
        compactions.erase(it1);
        if (compactions.empty()) { // No compactions on this tablet, erase key
            _submitted_cumu_compactions.erase(it);
            // No cumu compaction on this tablet, reset `last_cumu_no_suitable_version_ms` to enable this tablet to
            // enter the compaction scheduling candidate set. The purpose of doing this is to have at least one BE perform
            // cumu compaction on tablet which has suitable versions for cumu compaction.
            tablet->last_cumu_no_suitable_version_ms = 0;
        }
    };
    st = _cumu_compaction_thread_pool->submit_func([=, compaction = std::move(compaction)]() {
        auto st = compaction->execute_compact();
        if (!st.ok()) {
            // Error log has been output in `execute_compact`
            long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            tablet->set_last_cumu_compaction_failure_time(now);
        }
        erase_submitted_cumu_compaction();
    });
    if (!st.ok()) {
        erase_submitted_cumu_compaction();
        return Status::InternalError("failed to submit cumu compaction, tablet_id={}",
                                     tablet->tablet_id());
    }
    return st;
}

Status CloudStorageEngine::_submit_full_compaction_task(const CloudTabletSPtr& tablet) {
    using namespace std::chrono;
    {
        std::lock_guard lock(_compaction_mtx);
        // Take a placeholder for full compaction
        auto [_, success] = _submitted_full_compactions.emplace(tablet->tablet_id(), nullptr);
        if (!success) {
            return Status::AlreadyExist(
                    "other full compaction or base compaction is submitted, tablet_id={}",
                    tablet->tablet_id());
        }
    }
    //auto compaction = std::make_shared<CloudFullCompaction>(tablet);
    auto compaction = std::make_shared<CloudFullCompaction>(*this, tablet);
    auto st = compaction->prepare_compact();
    if (!st.ok()) {
        long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        tablet->set_last_full_compaction_failure_time(now);
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions.erase(tablet->tablet_id());
        return st;
    }
    {
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions[tablet->tablet_id()] = compaction;
    }
    st = _base_compaction_thread_pool->submit_func([=, this, compaction = std::move(compaction)]() {
        auto st = compaction->execute_compact();
        if (!st.ok()) {
            // Error log has been output in `execute_compact`
            long now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            tablet->set_last_full_compaction_failure_time(now);
        }
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions.erase(tablet->tablet_id());
    });
    if (!st.ok()) {
        std::lock_guard lock(_compaction_mtx);
        _submitted_full_compactions.erase(tablet->tablet_id());
        return Status::InternalError("failed to submit full compaction, tablet_id={}",
                                     tablet->tablet_id());
    }
    return st;
}

Status CloudStorageEngine::submit_compaction_task(const CloudTabletSPtr& tablet,
                                                  CompactionType compaction_type) {
    DCHECK(compaction_type == CompactionType::CUMULATIVE_COMPACTION ||
           compaction_type == CompactionType::BASE_COMPACTION ||
           compaction_type == CompactionType::FULL_COMPACTION);
    switch (compaction_type) {
    case CompactionType::BASE_COMPACTION:
        RETURN_IF_ERROR(_submit_base_compaction_task(tablet));
        return Status::OK();
    case CompactionType::CUMULATIVE_COMPACTION:
        RETURN_IF_ERROR(_submit_cumulative_compaction_task(tablet));
        return Status::OK();
    case CompactionType::FULL_COMPACTION:
        RETURN_IF_ERROR(_submit_full_compaction_task(tablet));
        return Status::OK();
    default:
        return Status::InternalError("unknown compaction type!");
    }
}

void CloudStorageEngine::_lease_compaction_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::lease_compaction_interval_seconds))) {
        std::vector<std::shared_ptr<CloudBaseCompaction>> base_compactions;
        std::vector<std::shared_ptr<CloudCumulativeCompaction>> cumu_compactions;
        {
            std::lock_guard lock(_compaction_mtx);
            for (auto& [_, base] : _submitted_base_compactions) {
                if (base) { // `base` might be a nullptr placeholder
                    base_compactions.push_back(base);
                }
            }
            for (auto& [_, cumus] : _submitted_cumu_compactions) {
                for (auto& cumu : cumus) {
                    cumu_compactions.push_back(cumu);
                }
            }
        }
        // TODO(plat1ko): Support batch lease rpc
        for (auto& comp : cumu_compactions) {
            comp->do_lease();
        }
        for (auto& comp : base_compactions) {
            comp->do_lease();
        }
    }
}

Status CloudStorageEngine::get_compaction_status_json(std::string* result) {
    rapidjson::Document root;
    root.SetObject();

    std::lock_guard lock(_compaction_mtx);
    // cumu
    std::string_view cumu = "CumulativeCompaction";
    rapidjson::Value cumu_key;
    cumu_key.SetString(cumu.data(), cumu.length(), root.GetAllocator());
    rapidjson::Document cumu_arr;
    cumu_arr.SetArray();
    for (auto& [tablet_id, v] : _submitted_cumu_compactions) {
        for (int i = 0; i < v.size(); ++i) {
            cumu_arr.PushBack(tablet_id, root.GetAllocator());
        }
    }
    root.AddMember(cumu_key, cumu_arr, root.GetAllocator());
    // base
    std::string_view base = "BaseCompaction";
    rapidjson::Value base_key;
    base_key.SetString(base.data(), base.length(), root.GetAllocator());
    rapidjson::Document base_arr;
    base_arr.SetArray();
    for (auto& [tablet_id, _] : _submitted_base_compactions) {
        base_arr.PushBack(tablet_id, root.GetAllocator());
    }
    root.AddMember(base_key, base_arr, root.GetAllocator());

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *result = std::string(strbuf.GetString());
    return Status::OK();
}

std::shared_ptr<CloudCumulativeCompactionPolicy> CloudStorageEngine::cumu_compaction_policy(
        std::string_view compaction_policy) {
    if (!_cumulative_compaction_policies.contains(compaction_policy)) {
        return _cumulative_compaction_policies.at(CUMULATIVE_SIZE_BASED_POLICY);
    }
    return _cumulative_compaction_policies.at(compaction_policy);
}

} // namespace doris
