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

#include "recycler/checker.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <fmt/core.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/bvars.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "recycler/hdfs_accessor.h"
#include "recycler/s3_accessor.h"
#include "recycler/storage_vault_accessor.h"
#ifdef UNIT_TEST
#include "../test/mock_accessor.h"
#endif
#include "recycler/util.h"

namespace doris::cloud {
namespace config {
extern int32_t brpc_listen_port;
extern int32_t scan_instances_interval_seconds;
extern int32_t recycle_job_lease_expired_ms;
extern int32_t recycle_concurrency;
extern std::vector<std::string> recycle_whitelist;
extern std::vector<std::string> recycle_blacklist;
extern bool enable_inverted_check;
} // namespace config

Checker::Checker(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {
    ip_port_ = std::string(butil::my_ip_cstr()) + ":" + std::to_string(config::brpc_listen_port);
}

Checker::~Checker() {
    if (!stopped()) {
        stop();
    }
}

int Checker::start() {
    DCHECK(txn_kv_);
    instance_filter_.reset(config::recycle_whitelist, config::recycle_blacklist);

    // launch instance scanner
    auto scanner_func = [this]() {
        std::this_thread::sleep_for(
                std::chrono::seconds(config::recycler_sleep_before_scheduling_seconds));
        while (!stopped()) {
            std::vector<InstanceInfoPB> instances;
            get_all_instances(txn_kv_.get(), instances);
            LOG(INFO) << "Checker get instances: " << [&instances] {
                std::stringstream ss;
                for (auto& i : instances) ss << ' ' << i.instance_id();
                return ss.str();
            }();
            if (!instances.empty()) {
                // enqueue instances
                std::lock_guard lock(mtx_);
                for (auto& instance : instances) {
                    if (instance_filter_.filter_out(instance.instance_id())) continue;
                    if (instance.status() == InstanceInfoPB::DELETED) continue;
                    using namespace std::chrono;
                    auto enqueue_time_s =
                            duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
                    auto [_, success] =
                            pending_instance_map_.insert({instance.instance_id(), enqueue_time_s});
                    // skip instance already in pending queue
                    if (success) {
                        pending_instance_queue_.push_back(std::move(instance));
                    }
                }
                pending_instance_cond_.notify_all();
            }
            {
                std::unique_lock lock(mtx_);
                notifier_.wait_for(lock,
                                   std::chrono::seconds(config::scan_instances_interval_seconds),
                                   [&]() { return stopped(); });
            }
        }
    };
    workers_.emplace_back(scanner_func);
    // Launch lease thread
    workers_.emplace_back([this] { lease_check_jobs(); });
    // Launch inspect thread
    workers_.emplace_back([this] { inspect_instance_check_interval(); });

    // launch check workers
    auto checker_func = [this]() {
        while (!stopped()) {
            // fetch instance to check
            InstanceInfoPB instance;
            long enqueue_time_s = 0;
            {
                std::unique_lock lock(mtx_);
                pending_instance_cond_.wait(lock, [&]() -> bool {
                    return !pending_instance_queue_.empty() || stopped();
                });
                if (stopped()) {
                    return;
                }
                instance = std::move(pending_instance_queue_.front());
                pending_instance_queue_.pop_front();
                enqueue_time_s = pending_instance_map_[instance.instance_id()];
                pending_instance_map_.erase(instance.instance_id());
            }
            const auto& instance_id = instance.instance_id();
            {
                std::lock_guard lock(mtx_);
                // skip instance in recycling
                if (working_instance_map_.count(instance_id)) continue;
            }
            auto checker = std::make_shared<InstanceChecker>(txn_kv_, instance.instance_id());
            if (checker->init(instance) != 0) {
                LOG(WARNING) << "failed to init instance checker, instance_id="
                             << instance.instance_id();
                continue;
            }
            std::string check_job_key;
            job_check_key({instance.instance_id()}, &check_job_key);
            int ret = prepare_instance_recycle_job(txn_kv_.get(), check_job_key,
                                                   instance.instance_id(), ip_port_,
                                                   config::check_object_interval_seconds * 1000);
            if (ret != 0) { // Prepare failed
                continue;
            } else {
                std::lock_guard lock(mtx_);
                working_instance_map_.emplace(instance_id, checker);
            }
            if (stopped()) return;
            using namespace std::chrono;
            auto ctime_ms =
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            g_bvar_checker_enqueue_cost_s.put(instance_id, ctime_ms / 1000 - enqueue_time_s);

            bool success {true};

            if (int ret = checker->do_check(); ret != 0) {
                success = false;
            }

            if (config::enable_inverted_check) {
                if (int ret = checker->do_inverted_check(); ret != 0) {
                    success = false;
                }
            }

            if (config::enable_delete_bitmap_inverted_check) {
                if (int ret = checker->do_delete_bitmap_inverted_check(); ret != 0) {
                    success = false;
                }
            }

            if (config::enable_delete_bitmap_storage_optimize_check) {
                if (int ret = checker->do_delete_bitmap_storage_optimize_check(); ret != 0) {
                    success = false;
                }
            }

            // If instance checker has been aborted, don't finish this job
            if (!checker->stopped()) {
                finish_instance_recycle_job(txn_kv_.get(), check_job_key, instance.instance_id(),
                                            ip_port_, success, ctime_ms);
            }
            {
                std::lock_guard lock(mtx_);
                working_instance_map_.erase(instance.instance_id());
            }
        }
    };
    int num_threads = config::recycle_concurrency; // FIXME: use a new config entry?
    for (int i = 0; i < num_threads; ++i) {
        workers_.emplace_back(checker_func);
    }
    return 0;
}

void Checker::stop() {
    stopped_ = true;
    notifier_.notify_all();
    pending_instance_cond_.notify_all();
    {
        std::lock_guard lock(mtx_);
        for (auto& [_, checker] : working_instance_map_) {
            checker->stop();
        }
    }
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

void Checker::lease_check_jobs() {
    while (!stopped()) {
        std::vector<std::string> instances;
        instances.reserve(working_instance_map_.size());
        {
            std::lock_guard lock(mtx_);
            for (auto& [id, _] : working_instance_map_) {
                instances.push_back(id);
            }
        }
        for (auto& i : instances) {
            std::string check_job_key;
            job_check_key({i}, &check_job_key);
            int ret = lease_instance_recycle_job(txn_kv_.get(), check_job_key, i, ip_port_);
            if (ret == 1) {
                std::lock_guard lock(mtx_);
                if (auto it = working_instance_map_.find(i); it != working_instance_map_.end()) {
                    it->second->stop();
                }
            }
        }
        {
            std::unique_lock lock(mtx_);
            notifier_.wait_for(lock,
                               std::chrono::milliseconds(config::recycle_job_lease_expired_ms / 3),
                               [&]() { return stopped(); });
        }
    }
}

#define LOG_CHECK_INTERVAL_ALARM LOG(WARNING) << "Err for check interval: "
void Checker::do_inspect(const InstanceInfoPB& instance) {
    std::string check_job_key = job_check_key({instance.instance_id()});
    std::unique_ptr<Transaction> txn;
    std::string val;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_CHECK_INTERVAL_ALARM << "failed to create txn";
        return;
    }
    err = txn->get(check_job_key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_CHECK_INTERVAL_ALARM << "failed to get kv, err=" << err
                                 << " key=" << hex(check_job_key);
        return;
    }
    auto checker = InstanceChecker(txn_kv_, instance.instance_id());
    if (checker.init(instance) != 0) {
        LOG_CHECK_INTERVAL_ALARM << "failed to init instance checker, instance_id="
                                 << instance.instance_id();
        return;
    }

    int64_t bucket_lifecycle_days = 0;
    if (checker.get_bucket_lifecycle(&bucket_lifecycle_days) != 0) {
        LOG_CHECK_INTERVAL_ALARM << "failed to get bucket lifecycle, instance_id="
                                 << instance.instance_id();
        return;
    }
    DCHECK(bucket_lifecycle_days > 0);

    if (bucket_lifecycle_days == INT64_MAX) {
        // No s3 bucket (may all accessors are HdfsAccessor), skip inspect
        return;
    }

    int64_t last_ctime_ms = -1;
    auto job_status = JobRecyclePB::IDLE;
    auto has_last_ctime = [&]() {
        JobRecyclePB job_info;
        if (!job_info.ParseFromString(val)) {
            LOG_CHECK_INTERVAL_ALARM << "failed to parse JobRecyclePB, key=" << hex(check_job_key);
        }
        DCHECK(job_info.instance_id() == instance.instance_id());
        if (!job_info.has_last_ctime_ms()) return false;
        last_ctime_ms = job_info.last_ctime_ms();
        job_status = job_info.status();
        g_bvar_checker_last_success_time_ms.put(instance.instance_id(),
                                                job_info.last_success_time_ms());
        return true;
    };
    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND || !has_last_ctime()) {
        // Use instance's ctime for instances that do not have job's last ctime
        last_ctime_ms = instance.ctime();
    }
    DCHECK(now - last_ctime_ms >= 0);
    int64_t expiration_ms =
            bucket_lifecycle_days > config::reserved_buffer_days
                    ? (bucket_lifecycle_days - config::reserved_buffer_days) * 86400000
                    : bucket_lifecycle_days * 86400000;
    TEST_SYNC_POINT_CALLBACK("Checker:do_inspect", &last_ctime_ms);
    if (now - last_ctime_ms >= expiration_ms) {
        LOG_CHECK_INTERVAL_ALARM << "check risks, instance_id: " << instance.instance_id()
                                 << " last_ctime_ms: " << last_ctime_ms
                                 << " job_status: " << job_status
                                 << " bucket_lifecycle_days: " << bucket_lifecycle_days
                                 << " reserved_buffer_days: " << config::reserved_buffer_days
                                 << " expiration_ms: " << expiration_ms;
    }
}
#undef LOG_CHECK_INTERVAL_ALARM
void Checker::inspect_instance_check_interval() {
    while (!stopped()) {
        LOG(INFO) << "start to inspect instance check interval";
        std::vector<InstanceInfoPB> instances;
        get_all_instances(txn_kv_.get(), instances);
        for (const auto& instance : instances) {
            if (instance_filter_.filter_out(instance.instance_id())) continue;
            if (stopped()) return;
            if (instance.status() == InstanceInfoPB::DELETED) continue;
            do_inspect(instance);
        }
        {
            std::unique_lock lock(mtx_);
            notifier_.wait_for(lock, std::chrono::seconds(config::scan_instances_interval_seconds),
                               [&]() { return stopped(); });
        }
    }
}

// return 0 for success get a key, 1 for key not found, negative for error
int key_exist(TxnKv* txn_kv, std::string_view key) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to init txn, err=" << err;
        return -1;
    }
    std::string val;
    switch (txn->get(key, &val)) {
    case TxnErrorCode::TXN_OK:
        return 0;
    case TxnErrorCode::TXN_KEY_NOT_FOUND:
        return 1;
    default:
        return -1;
    }
}

InstanceChecker::InstanceChecker(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id)
        : txn_kv_(std::move(txn_kv)), instance_id_(instance_id) {}

int InstanceChecker::init(const InstanceInfoPB& instance) {
    int ret = init_obj_store_accessors(instance);
    if (ret != 0) {
        return ret;
    }

    return init_storage_vault_accessors(instance);
}

int InstanceChecker::init_obj_store_accessors(const InstanceInfoPB& instance) {
    for (const auto& obj_info : instance.obj_info()) {
#ifdef UNIT_TEST
        auto accessor = std::make_shared<MockAccessor>();
#else
        auto s3_conf = S3Conf::from_obj_store_info(obj_info);
        if (!s3_conf) {
            LOG(WARNING) << "failed to init object accessor, instance_id=" << instance_id_;
            return -1;
        }

        std::shared_ptr<S3Accessor> accessor;
        int ret = S3Accessor::create(std::move(*s3_conf), &accessor);
        if (ret != 0) {
            LOG(WARNING) << "failed to init object accessor. instance_id=" << instance_id_
                         << " resource_id=" << obj_info.id();
            return ret;
        }
#endif

        accessor_map_.emplace(obj_info.id(), std::move(accessor));
    }

    return 0;
}

int InstanceChecker::init_storage_vault_accessors(const InstanceInfoPB& instance) {
    if (instance.resource_ids().empty()) {
        return 0;
    }

    FullRangeGetIteratorOptions opts(txn_kv_);
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(storage_vault_key({instance_id_, ""}),
                                      storage_vault_key({instance_id_, "\xff"}), std::move(opts));

    for (auto kv = it->next(); kv.has_value(); kv = it->next()) {
        auto [k, v] = *kv;
        StorageVaultPB vault;
        if (!vault.ParseFromArray(v.data(), v.size())) {
            LOG(WARNING) << "malformed storage vault, unable to deserialize key=" << hex(k);
            return -1;
        }

        if (vault.has_hdfs_info()) {
            auto accessor = std::make_shared<HdfsAccessor>(vault.hdfs_info());
            int ret = accessor->init();
            if (ret != 0) {
                LOG(WARNING) << "failed to init hdfs accessor. instance_id=" << instance_id_
                             << " resource_id=" << vault.id() << " name=" << vault.name();
                return ret;
            }

            accessor_map_.emplace(vault.id(), std::move(accessor));
        } else if (vault.has_obj_info()) {
#ifdef UNIT_TEST
            auto accessor = std::make_shared<MockAccessor>();
#else
            auto s3_conf = S3Conf::from_obj_store_info(vault.obj_info());
            if (!s3_conf) {
                LOG(WARNING) << "failed to init object accessor, instance_id=" << instance_id_;
                return -1;
            }

            std::shared_ptr<S3Accessor> accessor;
            int ret = S3Accessor::create(std::move(*s3_conf), &accessor);
            if (ret != 0) {
                LOG(WARNING) << "failed to init s3 accessor. instance_id=" << instance_id_
                             << " resource_id=" << vault.id() << " name=" << vault.name();
                return ret;
            }
#endif

            accessor_map_.emplace(vault.id(), std::move(accessor));
        }
    }

    if (!it->is_valid()) {
        LOG_WARNING("failed to get storage vault kv");
        return -1;
    }
    return 0;
}

int InstanceChecker::do_check() {
    TEST_SYNC_POINT("InstanceChecker.do_check");
    LOG(INFO) << "begin to check instance objects instance_id=" << instance_id_;
    int check_ret = 0;
    long num_scanned = 0;
    long num_scanned_with_segment = 0;
    long num_rowset_loss = 0;
    long instance_volume = 0;
    using namespace std::chrono;
    auto start_time = steady_clock::now();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << "check instance objects finished, cost=" << cost
                  << "s. instance_id=" << instance_id_ << " num_scanned=" << num_scanned
                  << " num_scanned_with_segment=" << num_scanned_with_segment
                  << " num_rowset_loss=" << num_rowset_loss
                  << " instance_volume=" << instance_volume;
        g_bvar_checker_num_scanned.put(instance_id_, num_scanned);
        g_bvar_checker_num_scanned_with_segment.put(instance_id_, num_scanned_with_segment);
        g_bvar_checker_num_check_failed.put(instance_id_, num_rowset_loss);
        g_bvar_checker_check_cost_s.put(instance_id_, static_cast<long>(cost));
        // FIXME(plat1ko): What if some list operation failed?
        g_bvar_checker_instance_volume.put(instance_id_, instance_volume);
    });

    struct TabletFiles {
        int64_t tablet_id {0};
        std::unordered_set<std::string> files;
    };
    TabletFiles tablet_files_cache;

    auto check_rowset_objects = [&, this](const doris::RowsetMetaCloudPB& rs_meta,
                                          std::string_view key) {
        if (rs_meta.num_segments() == 0) {
            return;
        }

        ++num_scanned_with_segment;
        if (tablet_files_cache.tablet_id != rs_meta.tablet_id()) {
            long tablet_volume = 0;
            // Clear cache
            tablet_files_cache.tablet_id = 0;
            tablet_files_cache.files.clear();
            // Get all file paths under this tablet directory
            auto find_it = accessor_map_.find(rs_meta.resource_id());
            if (find_it == accessor_map_.end()) {
                LOG_WARNING("resource id not found in accessor map")
                        .tag("resource_id", rs_meta.resource_id())
                        .tag("tablet_id", rs_meta.tablet_id())
                        .tag("rowset_id", rs_meta.rowset_id_v2());
                check_ret = -1;
                return;
            }

            std::unique_ptr<ListIterator> list_iter;
            int ret = find_it->second->list_directory(tablet_path_prefix(rs_meta.tablet_id()),
                                                      &list_iter);
            if (ret != 0) { // No need to log, because S3Accessor has logged this error
                check_ret = -1;
                return;
            }

            for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
                tablet_files_cache.files.insert(std::move(file->path));
                tablet_volume += file->size;
            }
            tablet_files_cache.tablet_id = rs_meta.tablet_id();
            instance_volume += tablet_volume;
        }

        bool data_loss = false;
        for (int i = 0; i < rs_meta.num_segments(); ++i) {
            auto path = segment_path(rs_meta.tablet_id(), rs_meta.rowset_id_v2(), i);
            if (tablet_files_cache.files.contains(path)) {
                continue;
            }

            if (1 == key_exist(txn_kv_.get(), key)) {
                // Rowset has been deleted instead of data loss
                break;
            }
            data_loss = true;
            TEST_SYNC_POINT_CALLBACK("InstanceChecker.do_check1", &path);
            LOG(WARNING) << "object not exist, path=" << path << " key=" << hex(key);
        }

        if (data_loss) {
            ++num_rowset_loss;
        }
    };

    // scan visible rowsets
    auto start_key = meta_rowset_key({instance_id_, 0, 0});
    auto end_key = meta_rowset_key({instance_id_, INT64_MAX, 0});

    std::unique_ptr<RangeGetIterator> it;
    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to init txn, err=" << err;
            return -1;
        }

        err = txn->get(start_key, end_key, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "internal error, failed to get rowset meta, err=" << err;
            return -1;
        }
        num_scanned += it->size();

        while (it->has_next() && !stopped()) {
            auto [k, v] = it->next();
            if (!it->has_next()) start_key = k;

            doris::RowsetMetaCloudPB rs_meta;
            if (!rs_meta.ParseFromArray(v.data(), v.size())) {
                ++num_rowset_loss;
                LOG(WARNING) << "malformed rowset meta. key=" << hex(k) << " val=" << hex(v);
                continue;
            }
            check_rowset_objects(rs_meta, k);
        }
        start_key.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more() && !stopped());

    return num_rowset_loss > 0 ? 1 : check_ret;
}

int InstanceChecker::get_bucket_lifecycle(int64_t* lifecycle_days) {
    // If there are multiple buckets, return the minimum lifecycle.
    int64_t min_lifecycle_days = INT64_MAX;
    int64_t tmp_liefcycle_days = 0;
    for (const auto& [id, accessor] : accessor_map_) {
        if (accessor->type() != AccessorType::S3) {
            continue;
        }

        auto* s3_accessor = static_cast<S3Accessor*>(accessor.get());

        if (s3_accessor->check_versioning() != 0) {
            return -1;
        }

        if (s3_accessor->get_life_cycle(&tmp_liefcycle_days) != 0) {
            return -1;
        }

        if (tmp_liefcycle_days < min_lifecycle_days) {
            min_lifecycle_days = tmp_liefcycle_days;
        }
    }
    *lifecycle_days = min_lifecycle_days;
    return 0;
}

int InstanceChecker::do_inverted_check() {
    if (accessor_map_.size() > 1) {
        LOG(INFO) << "currently not support inverted check for multi accessor. instance_id="
                  << instance_id_;
        return 0;
    }

    LOG(INFO) << "begin to inverted check objects instance_id=" << instance_id_;
    int check_ret = 0;
    long num_scanned = 0;
    long num_file_leak = 0;
    using namespace std::chrono;
    auto start_time = steady_clock::now();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        g_bvar_inverted_checker_num_scanned.put(instance_id_, num_scanned);
        g_bvar_inverted_checker_num_check_failed.put(instance_id_, num_file_leak);
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << "inverted check instance objects finished, cost=" << cost
                  << "s. instance_id=" << instance_id_ << " num_scanned=" << num_scanned
                  << " num_file_leak=" << num_file_leak;
    });

    struct TabletRowsets {
        int64_t tablet_id {0};
        std::unordered_set<std::string> rowset_ids;
    };
    TabletRowsets tablet_rowsets_cache;

    // Return 0 if check success, return 1 if file is garbage data, negative if error occurred
    auto check_segment_file = [&](const std::string& obj_key) {
        std::vector<std::string> str;
        butil::SplitString(obj_key, '/', &str);
        // data/{tablet_id}/{rowset_id}_{seg_num}.dat
        if (str.size() < 3) {
            return -1;
        }

        int64_t tablet_id = atol(str[1].c_str());
        if (tablet_id <= 0) {
            LOG(WARNING) << "failed to parse tablet_id, key=" << obj_key;
            return -1;
        }

        std::string rowset_id;
        if (auto pos = str.back().find('_'); pos != std::string::npos) {
            rowset_id = str.back().substr(0, pos);
        } else {
            LOG(WARNING) << "failed to parse rowset_id, key=" << obj_key;
            return -1;
        }

        if (tablet_rowsets_cache.tablet_id == tablet_id) {
            if (tablet_rowsets_cache.rowset_ids.contains(rowset_id)) {
                return 0;
            } else {
                LOG(WARNING) << "rowset not exists, key=" << obj_key;
                return -1;
            }
        }
        // Get all rowset id of this tablet
        tablet_rowsets_cache.tablet_id = tablet_id;
        tablet_rowsets_cache.rowset_ids.clear();
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }
        std::unique_ptr<RangeGetIterator> it;
        auto begin = meta_rowset_key({instance_id_, tablet_id, 0});
        auto end = meta_rowset_key({instance_id_, tablet_id, INT64_MAX});
        do {
            TxnErrorCode err = txn->get(begin, end, &it);
            if (err != TxnErrorCode::TXN_OK) {
                LOG(WARNING) << "failed to get rowset kv, err=" << err;
                return -1;
            }
            if (!it->has_next()) {
                break;
            }
            while (it->has_next()) {
                // recycle corresponding resources
                auto [k, v] = it->next();
                doris::RowsetMetaCloudPB rowset;
                if (!rowset.ParseFromArray(v.data(), v.size())) {
                    LOG(WARNING) << "malformed rowset meta value, key=" << hex(k);
                    return -1;
                }
                tablet_rowsets_cache.rowset_ids.insert(rowset.rowset_id_v2());
                if (!it->has_next()) {
                    begin = k;
                    begin.push_back('\x00'); // Update to next smallest key for iteration
                    break;
                }
            }
        } while (it->more() && !stopped());

        if (!tablet_rowsets_cache.rowset_ids.contains(rowset_id)) {
            // Garbage data leak
            LOG(WARNING) << "rowset should be recycled, key=" << obj_key;
            return 1;
        }

        return 0;
    };

    // TODO(Xiaocc): Currently we haven't implemented one generator-like s3 accessor list function
    // so we choose to skip here.
    TEST_SYNC_POINT_RETURN_WITH_VALUE("InstanceChecker::do_inverted_check", (int)0);

    for (auto& [_, accessor] : accessor_map_) {
        std::unique_ptr<ListIterator> list_iter;
        int ret = accessor->list_directory("data", &list_iter);
        if (ret != 0) {
            return -1;
        }

        for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
            ++num_scanned;
            int ret = check_segment_file(file->path);
            if (ret != 0) {
                LOG(WARNING) << "failed to check segment file, uri=" << accessor->uri()
                             << " path=" << file->path;
                if (ret == 1) {
                    ++num_file_leak;
                } else {
                    check_ret = -1;
                }
            }
        }

        if (!list_iter->is_valid()) {
            LOG(WARNING) << "failed to list data directory. uri=" << accessor->uri();
            return -1;
        }
    }
    return num_file_leak > 0 ? 1 : check_ret;
}

int InstanceChecker::traverse_mow_tablet(const std::function<int(int64_t)>& check_func) {
    std::unique_ptr<RangeGetIterator> it;
    auto begin = meta_rowset_key({instance_id_, 0, 0});
    auto end = meta_rowset_key({instance_id_, std::numeric_limits<int64_t>::max(), 0});
    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }
        err = txn->get(begin, end, &it, false, 1);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get rowset kv, err=" << err;
            return -1;
        }
        if (!it->has_next()) {
            break;
        }
        while (it->has_next() && !stopped()) {
            auto [k, v] = it->next();
            std::string_view k1 = k;
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version} -> RowsetMetaCloudPB
            auto tablet_id = std::get<int64_t>(std::get<0>(out[3]));

            if (!it->has_next()) {
                // Update to next smallest key for iteration
                // scan for next tablet in this instance
                begin = meta_rowset_key({instance_id_, tablet_id + 1, 0});
            }

            TabletMetaCloudPB tablet_meta;
            int ret = get_tablet_meta(txn_kv_.get(), instance_id_, tablet_id, tablet_meta);
            if (ret < 0) {
                LOG(WARNING) << fmt::format(
                        "failed to get_tablet_meta in do_delete_bitmap_integrity_check(), "
                        "instance_id={}, tablet_id={}",
                        instance_id_, tablet_id);
                return ret;
            }

            if (tablet_meta.enable_unique_key_merge_on_write()) {
                // only check merge-on-write table
                int ret = check_func(tablet_id);
                if (ret < 0) {
                    // return immediately when encounter unexpected error,
                    // otherwise, we continue to check the next tablet
                    return ret;
                }
            }
        }
    } while (it->more() && !stopped());
    return 0;
}

int InstanceChecker::traverse_rowset_delete_bitmaps(
        int64_t tablet_id, std::string rowset_id,
        const std::function<int(int64_t, std::string_view, int64_t, int64_t)>& callback) {
    std::unique_ptr<RangeGetIterator> it;
    auto begin = meta_delete_bitmap_key({instance_id_, tablet_id, rowset_id, 0, 0});
    auto end = meta_delete_bitmap_key({instance_id_, tablet_id, rowset_id,
                                       std::numeric_limits<int64_t>::max(),
                                       std::numeric_limits<int64_t>::max()});
    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }
        err = txn->get(begin, end, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get rowset kv, err=" << err;
            return -1;
        }
        if (!it->has_next()) {
            break;
        }
        while (it->has_next() && !stopped()) {
            auto [k, v] = it->next();
            std::string_view k1 = k;
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "meta" ${instance_id} "delete_bitmap" ${tablet_id} ${rowset_id} ${version} ${segment_id} -> roaringbitmap
            auto version = std::get<std::int64_t>(std::get<0>(out[5]));
            auto segment_id = std::get<std::int64_t>(std::get<0>(out[6]));

            int ret = callback(tablet_id, rowset_id, version, segment_id);
            if (ret != 0) {
                return ret;
            }

            if (!it->has_next()) {
                begin = k;
                begin.push_back('\x00'); // Update to next smallest key for iteration
                break;
            }
        }
    } while (it->more() && !stopped());

    return 0;
}

int InstanceChecker::collect_tablet_rowsets(
        int64_t tablet_id, const std::function<void(const doris::RowsetMetaCloudPB&)>& collect_cb) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    std::unique_ptr<RangeGetIterator> it;
    auto begin = meta_rowset_key({instance_id_, tablet_id, 0});
    auto end = meta_rowset_key({instance_id_, tablet_id + 1, 0});

    int64_t rowsets_num {0};
    do {
        TxnErrorCode err = txn->get(begin, end, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get rowset kv, err=" << err;
            return -1;
        }
        if (!it->has_next()) {
            break;
        }
        while (it->has_next() && !stopped()) {
            auto [k, v] = it->next();
            doris::RowsetMetaCloudPB rowset;
            if (!rowset.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed rowset meta value, key=" << hex(k);
                return -1;
            }

            ++rowsets_num;
            collect_cb(rowset);

            if (!it->has_next()) {
                begin = k;
                begin.push_back('\x00'); // Update to next smallest key for iteration
                break;
            }
        }
    } while (it->more() && !stopped());

    LOG(INFO) << fmt::format(
            "[delete bitmap checker] successfully collect rowsets for instance_id={}, "
            "tablet_id={}, rowsets_num={}",
            instance_id_, tablet_id, rowsets_num);
    return 0;
}

int InstanceChecker::do_delete_bitmap_inverted_check() {
    LOG(INFO) << fmt::format(
            "[delete bitmap checker] begin to do_delete_bitmap_inverted_check for instance_id={}",
            instance_id_);

    // number of delete bitmap keys being scanned
    int64_t total_delete_bitmap_keys {0};
    // number of delete bitmaps which belongs to non mow tablet
    int64_t abnormal_delete_bitmaps {0};
    // number of delete bitmaps which doesn't have corresponding rowset in MS
    int64_t leaked_delete_bitmaps {0};

    auto start_time = std::chrono::steady_clock::now();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        g_bvar_inverted_checker_leaked_delete_bitmaps.put(instance_id_, leaked_delete_bitmaps);
        g_bvar_inverted_checker_abnormal_delete_bitmaps.put(instance_id_, abnormal_delete_bitmaps);
        g_bvar_inverted_checker_delete_bitmaps_scanned.put(instance_id_, total_delete_bitmap_keys);

        auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start_time)
                            .count();
        if (leaked_delete_bitmaps > 0 || abnormal_delete_bitmaps > 0) {
            LOG(WARNING) << fmt::format(
                    "[delete bitmap check fails] delete bitmap inverted check for instance_id={}, "
                    "cost={} ms, total_delete_bitmap_keys={}, leaked_delete_bitmaps={}, "
                    "abnormal_delete_bitmaps={}",
                    instance_id_, cost, total_delete_bitmap_keys, leaked_delete_bitmaps,
                    abnormal_delete_bitmaps);
        } else {
            LOG(INFO) << fmt::format(
                    "[delete bitmap checker] delete bitmap inverted check for instance_id={}, "
                    "passed. cost={} ms, total_delete_bitmap_keys={}",
                    instance_id_, cost, total_delete_bitmap_keys);
        }
    });

    struct TabletsRowsetsCache {
        int64_t tablet_id {-1};
        bool enable_merge_on_write {false};
        std::unordered_set<std::string> rowsets {};
    } tablet_rowsets_cache {};

    std::unique_ptr<RangeGetIterator> it;
    auto begin = meta_delete_bitmap_key({instance_id_, 0, "", 0, 0});
    auto end =
            meta_delete_bitmap_key({instance_id_, std::numeric_limits<int64_t>::max(), "", 0, 0});
    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }
        err = txn->get(begin, end, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get rowset kv, err=" << err;
            return -1;
        }
        if (!it->has_next()) {
            break;
        }
        while (it->has_next() && !stopped()) {
            auto [k, v] = it->next();
            std::string_view k1 = k;
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "meta" ${instance_id} "delete_bitmap" ${tablet_id} ${rowset_id} ${version} ${segment_id} -> roaringbitmap
            auto tablet_id = std::get<int64_t>(std::get<0>(out[3]));
            auto rowset_id = std::get<std::string>(std::get<0>(out[4]));
            auto version = std::get<std::int64_t>(std::get<0>(out[5]));
            auto segment_id = std::get<std::int64_t>(std::get<0>(out[6]));

            ++total_delete_bitmap_keys;

            if (!it->has_next()) {
                begin = k;
                begin.push_back('\x00'); // Update to next smallest key for iteration
            }

            if (tablet_rowsets_cache.tablet_id == -1 ||
                tablet_rowsets_cache.tablet_id != tablet_id) {
                TabletMetaCloudPB tablet_meta;
                int ret = get_tablet_meta(txn_kv_.get(), instance_id_, tablet_id, tablet_meta);
                if (ret < 0) {
                    LOG(WARNING) << fmt::format(
                            "[delete bitmap checker] failed to get_tablet_meta in "
                            "do_delete_bitmap_inverted_check(), instance_id={}, tablet_id={}",
                            instance_id_, tablet_id);
                    return ret;
                }

                tablet_rowsets_cache.tablet_id = tablet_id;
                tablet_rowsets_cache.enable_merge_on_write =
                        tablet_meta.enable_unique_key_merge_on_write();
                tablet_rowsets_cache.rowsets.clear();

                if (tablet_rowsets_cache.enable_merge_on_write) {
                    // only collect rowsets for merge-on-write tablet
                    auto collect_cb =
                            [&tablet_rowsets_cache](const doris::RowsetMetaCloudPB& rowset) {
                                tablet_rowsets_cache.rowsets.insert(rowset.rowset_id_v2());
                            };
                    ret = collect_tablet_rowsets(tablet_id, collect_cb);
                    if (ret < 0) {
                        return ret;
                    }
                }
            }
            DCHECK_EQ(tablet_id, tablet_rowsets_cache.tablet_id);

            if (!tablet_rowsets_cache.enable_merge_on_write) {
                // clang-format off
                TEST_SYNC_POINT_CALLBACK(
                        "InstanceChecker::do_delete_bitmap_inverted_check.get_abnormal_delete_bitmap",
                        &tablet_id, &rowset_id, &version, &segment_id);
                // clang-format on
                ++abnormal_delete_bitmaps;
                // log an error and continue to check the next delete bitmap
                LOG(WARNING) << fmt::format(
                        "[delete bitmap check fails] find a delete bitmap belongs to tablet "
                        "which is not a merge-on-write table! instance_id={}, tablet_id={}, "
                        "version={}, segment_id={}",
                        instance_id_, tablet_id, version, segment_id);
                continue;
            }

            if (!tablet_rowsets_cache.rowsets.contains(rowset_id)) {
                TEST_SYNC_POINT_CALLBACK(
                        "InstanceChecker::do_delete_bitmap_inverted_check.get_leaked_delete_bitmap",
                        &tablet_id, &rowset_id, &version, &segment_id);
                ++leaked_delete_bitmaps;
                // log an error and continue to check the next delete bitmap
                LOG(WARNING) << fmt::format(
                        "[delete bitmap check fails] can't find corresponding rowset for delete "
                        "bitmap instance_id={}, tablet_id={}, rowset_id={}, version={}, "
                        "segment_id={}",
                        instance_id_, tablet_id, rowset_id, version, segment_id);
            }
        }
    } while (it->more() && !stopped());

    return (leaked_delete_bitmaps > 0 || abnormal_delete_bitmaps > 0) ? 1 : 0;
}

int InstanceChecker::check_delete_bitmap_storage_optimize(int64_t tablet_id) {
    using Version = std::pair<int64_t, int64_t>;
    struct RowsetDigest {
        std::string rowset_id;
        Version version;
        doris::SegmentsOverlapPB segments_overlap;

        bool operator<(const RowsetDigest& other) const {
            return version.first < other.version.first;
        }

        bool produced_by_compaction() const {
            return (version.first < version.second) ||
                   ((version.first == version.second) && segments_overlap == NONOVERLAPPING);
        }
    };

    // number of rowsets which may have problems
    int64_t abnormal_rowsets_num {0};

    std::vector<RowsetDigest> tablet_rowsets {};
    // Get all visible rowsets of this tablet
    auto collect_cb = [&tablet_rowsets](const doris::RowsetMetaCloudPB& rowset) {
        if (rowset.start_version() == 0 && rowset.end_version() == 1) {
            // ignore dummy rowset [0-1]
            return;
        }
        tablet_rowsets.emplace_back(
                rowset.rowset_id_v2(),
                std::make_pair<int64_t, int64_t>(rowset.start_version(), rowset.end_version()),
                rowset.segments_overlap_pb());
    };
    if (int ret = collect_tablet_rowsets(tablet_id, collect_cb); ret != 0) {
        return ret;
    }

    std::sort(tablet_rowsets.begin(), tablet_rowsets.end());

    // find right-most rowset which is produced by compaction
    auto it = std::find_if(
            tablet_rowsets.crbegin(), tablet_rowsets.crend(),
            [](const RowsetDigest& rowset) { return rowset.produced_by_compaction(); });
    if (it == tablet_rowsets.crend()) {
        LOG(INFO) << fmt::format(
                "[delete bitmap checker] skip to check delete bitmap storage optimize for "
                "tablet_id={} because it doesn't have compacted rowsets.",
                tablet_id);
        return 0;
    }

    int64_t start_version = it->version.first;
    int64_t pre_min_version = it->version.second;

    // after BE sweeping stale rowsets, all rowsets in this tablet before
    // should not have delete bitmaps with versions lower than `pre_min_version`
    if (config::delete_bitmap_storage_optimize_check_version_gap > 0) {
        pre_min_version -= config::delete_bitmap_storage_optimize_check_version_gap;
        if (pre_min_version <= 1) {
            LOG(INFO) << fmt::format(
                    "[delete bitmap checker] skip to check delete bitmap storage optimize for "
                    "tablet_id={} because pre_min_version is too small.",
                    tablet_id);
            return 0;
        }
    }

    auto check_func = [pre_min_version, instance_id = instance_id_](
                              int64_t tablet_id, std::string_view rowset_id, int64_t version,
                              int64_t segment_id) -> int {
        if (version < pre_min_version) {
            LOG(WARNING) << fmt::format(
                    "[delete bitmap check fails] delete bitmap storage optimize check fail for "
                    "instance_id={}, tablet_id={}, rowset_id={}, found delete bitmap with "
                    "version={} < pre_min_version={}",
                    instance_id, tablet_id, rowset_id, version, pre_min_version);
            return 1;
        }
        return 0;
    };

    for (const auto& rowset : tablet_rowsets) {
        // check for all rowsets before the max compacted rowset
        if (rowset.version.second < start_version) {
            auto rowset_id = rowset.rowset_id;
            int ret = traverse_rowset_delete_bitmaps(tablet_id, rowset_id, check_func);
            if (ret < 0) {
                return ret;
            }

            if (ret != 0) {
                ++abnormal_rowsets_num;
                TEST_SYNC_POINT_CALLBACK(
                        "InstanceChecker::check_delete_bitmap_storage_optimize.get_abnormal_rowset",
                        &tablet_id, &rowset_id);
            }
        }
    }

    LOG(INFO) << fmt::format(
            "[delete bitmap checker] finish check delete bitmap storage optimize for "
            "instance_id={}, tablet_id={}, rowsets_num={}, abnormal_rowsets_num={}, "
            "pre_min_version={}",
            instance_id_, tablet_id, tablet_rowsets.size(), abnormal_rowsets_num, pre_min_version);

    return (abnormal_rowsets_num > 1 ? 1 : 0);
}

int InstanceChecker::do_delete_bitmap_storage_optimize_check() {
    int64_t total_tablets_num {0};
    int64_t failed_tablets_num {0};

    // check that for every visible rowset, there exists at least delete one bitmap in MS
    int ret = traverse_mow_tablet([&](int64_t tablet_id) {
        ++total_tablets_num;
        int res = check_delete_bitmap_storage_optimize(tablet_id);
        failed_tablets_num += (res != 0);
        return res;
    });

    if (ret < 0) {
        return ret;
    }

    LOG(INFO) << fmt::format(
            "[delete bitmap checker] check delete bitmap storage optimize for instance_id={}, "
            "total_tablets_num={}, failed_tablets_num={}",
            instance_id_, total_tablets_num, failed_tablets_num);

    return (failed_tablets_num > 0) ? 1 : 0;
}

} // namespace doris::cloud
