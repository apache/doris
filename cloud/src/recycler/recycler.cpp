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

#include "recycler/recycler.h"

#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <string>
#include <string_view>

#include "common/stopwatch.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "recycler/checker.h"
#include "recycler/hdfs_accessor.h"
#include "recycler/s3_accessor.h"
#include "recycler/storage_vault_accessor.h"
#ifdef UNIT_TEST
#include "../test/mock_accessor.h"
#endif
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "recycler/recycler_service.h"
#include "recycler/util.h"

namespace doris::cloud {

using namespace std::chrono;

// return 0 for success get a key, 1 for key not found, negative for error
[[maybe_unused]] static int txn_get(TxnKv* txn_kv, std::string_view key, std::string& val) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return -1;
    }
    switch (txn->get(key, &val, true)) {
    case TxnErrorCode::TXN_OK:
        return 0;
    case TxnErrorCode::TXN_KEY_NOT_FOUND:
        return 1;
    default:
        return -1;
    };
}

// 0 for success, negative for error
static int txn_get(TxnKv* txn_kv, std::string_view begin, std::string_view end,
                   std::unique_ptr<RangeGetIterator>& it) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return -1;
    }
    switch (txn->get(begin, end, &it, true)) {
    case TxnErrorCode::TXN_OK:
        return 0;
    case TxnErrorCode::TXN_KEY_NOT_FOUND:
        return 1;
    default:
        return -1;
    };
}

// return 0 for success otherwise error
static int txn_remove(TxnKv* txn_kv, std::vector<std::string_view> keys) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return -1;
    }
    for (auto k : keys) {
        txn->remove(k);
    }
    switch (txn->commit()) {
    case TxnErrorCode::TXN_OK:
        return 0;
    case TxnErrorCode::TXN_CONFLICT:
        return -1;
    default:
        return -1;
    }
}

// return 0 for success otherwise error
static int txn_remove(TxnKv* txn_kv, std::vector<std::string> keys) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return -1;
    }
    for (auto& k : keys) {
        txn->remove(k);
    }
    switch (txn->commit()) {
    case TxnErrorCode::TXN_OK:
        return 0;
    case TxnErrorCode::TXN_CONFLICT:
        return -1;
    default:
        return -1;
    }
}

// return 0 for success otherwise error
[[maybe_unused]] static int txn_remove(TxnKv* txn_kv, std::string_view begin,
                                       std::string_view end) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->remove(begin, end);
    switch (txn->commit()) {
    case TxnErrorCode::TXN_OK:
        return 0;
    case TxnErrorCode::TXN_CONFLICT:
        return -1;
    default:
        return -1;
    }
}

static inline void check_recycle_task(const std::string& instance_id, const std::string& task_name,
                                      int64_t num_scanned, int64_t num_recycled,
                                      int64_t start_time) {
    if ((num_scanned % 10000) == 0 && (num_scanned > 0)) [[unlikely]] {
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        if (cost > config::recycle_task_threshold_seconds) {
            LOG_INFO("recycle task cost too much time cost={}s", cost)
                    .tag("instance_id", instance_id)
                    .tag("task", task_name)
                    .tag("num_scanned", num_scanned)
                    .tag("num_recycled", num_recycled);
        }
    }
    return;
}

Recycler::Recycler(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {
    ip_port_ = std::string(butil::my_ip_cstr()) + ":" + std::to_string(config::brpc_listen_port);
}

Recycler::~Recycler() {
    if (!stopped()) {
        stop();
    }
}

void Recycler::instance_scanner_callback() {
    while (!stopped()) {
        std::vector<InstanceInfoPB> instances;
        get_all_instances(txn_kv_.get(), instances);
        // TODO(plat1ko): delete job recycle kv of non-existent instances
        LOG(INFO) << "Recycler get instances: " << [&instances] {
            std::stringstream ss;
            for (auto& i : instances) ss << ' ' << i.instance_id();
            return ss.str();
        }();
        if (!instances.empty()) {
            // enqueue instances
            std::lock_guard lock(mtx_);
            for (auto& instance : instances) {
                if (instance_filter_.filter_out(instance.instance_id())) continue;
                auto [_, success] = pending_instance_set_.insert(instance.instance_id());
                // skip instance already in pending queue
                if (success) {
                    pending_instance_queue_.push_back(std::move(instance));
                }
            }
            pending_instance_cond_.notify_all();
        }
        {
            std::unique_lock lock(mtx_);
            notifier_.wait_for(lock, std::chrono::seconds(config::recycle_interval_seconds),
                               [&]() { return stopped(); });
        }
    }
}

void Recycler::recycle_callback() {
    while (!stopped()) {
        InstanceInfoPB instance;
        {
            std::unique_lock lock(mtx_);
            pending_instance_cond_.wait(
                    lock, [&]() { return !pending_instance_queue_.empty() || stopped(); });
            if (stopped()) {
                return;
            }
            instance = std::move(pending_instance_queue_.front());
            pending_instance_queue_.pop_front();
            pending_instance_set_.erase(instance.instance_id());
        }
        auto& instance_id = instance.instance_id();
        {
            std::lock_guard lock(mtx_);
            // skip instance in recycling
            if (recycling_instance_map_.count(instance_id)) continue;
        }
        auto instance_recycler = std::make_shared<InstanceRecycler>(txn_kv_, instance);
        if (instance_recycler->init() != 0) {
            LOG(WARNING) << "failed to init instance recycler, instance_id=" << instance_id;
            continue;
        }
        std::string recycle_job_key;
        job_recycle_key({instance_id}, &recycle_job_key);
        int ret = prepare_instance_recycle_job(txn_kv_.get(), recycle_job_key, instance_id,
                                               ip_port_, config::recycle_interval_seconds * 1000);
        if (ret != 0) { // Prepare failed
            continue;
        } else {
            std::lock_guard lock(mtx_);
            recycling_instance_map_.emplace(instance_id, instance_recycler);
        }
        if (stopped()) return;
        LOG_INFO("begin to recycle instance").tag("instance_id", instance_id);
        auto ctime_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        ret = instance_recycler->do_recycle();
        // If instance recycler has been aborted, don't finish this job
        if (!instance_recycler->stopped()) {
            finish_instance_recycle_job(txn_kv_.get(), recycle_job_key, instance_id, ip_port_,
                                        ret == 0, ctime_ms);
        }
        {
            std::lock_guard lock(mtx_);
            recycling_instance_map_.erase(instance_id);
        }
        LOG_INFO("finish recycle instance").tag("instance_id", instance_id);
    }
}

void Recycler::lease_recycle_jobs() {
    while (!stopped()) {
        std::vector<std::string> instances;
        instances.reserve(recycling_instance_map_.size());
        {
            std::lock_guard lock(mtx_);
            for (auto& [id, _] : recycling_instance_map_) {
                instances.push_back(id);
            }
        }
        for (auto& i : instances) {
            std::string recycle_job_key;
            job_recycle_key({i}, &recycle_job_key);
            int ret = lease_instance_recycle_job(txn_kv_.get(), recycle_job_key, i, ip_port_);
            if (ret == 1) {
                std::lock_guard lock(mtx_);
                if (auto it = recycling_instance_map_.find(i);
                    it != recycling_instance_map_.end()) {
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

void Recycler::check_recycle_tasks() {
    while (!stopped()) {
        std::unordered_map<std::string, std::shared_ptr<InstanceRecycler>> recycling_instance_map;
        {
            std::lock_guard lock(mtx_);
            recycling_instance_map = recycling_instance_map_;
        }
        for (auto& entry : recycling_instance_map) {
            entry.second->check_recycle_tasks();
        }

        std::unique_lock lock(mtx_);
        notifier_.wait_for(lock, std::chrono::seconds(config::check_recycle_task_interval_seconds),
                           [&]() { return stopped(); });
    }
}

int Recycler::start(brpc::Server* server) {
    instance_filter_.reset(config::recycle_whitelist, config::recycle_blacklist);

    if (config::enable_checker) {
        checker_ = std::make_unique<Checker>(txn_kv_);
        int ret = checker_->start();
        std::string msg;
        if (ret != 0) {
            msg = "failed to start checker";
            LOG(ERROR) << msg;
            std::cerr << msg << std::endl;
            return ret;
        }
        msg = "checker started";
        LOG(INFO) << msg;
        std::cout << msg << std::endl;
    }

    if (server) {
        // Add service
        auto recycler_service = new RecyclerServiceImpl(txn_kv_, this, checker_.get());
        server->AddService(recycler_service, brpc::SERVER_OWNS_SERVICE);
    }

    workers_.emplace_back([this] { instance_scanner_callback(); });
    for (int i = 0; i < config::recycle_concurrency; ++i) {
        workers_.emplace_back([this] { recycle_callback(); });
    }

    workers_.emplace_back(std::mem_fn(&Recycler::lease_recycle_jobs), this);
    workers_.emplace_back(std::mem_fn(&Recycler::check_recycle_tasks), this);
    return 0;
}

void Recycler::stop() {
    stopped_ = true;
    notifier_.notify_all();
    pending_instance_cond_.notify_all();
    {
        std::lock_guard lock(mtx_);
        for (auto& [_, recycler] : recycling_instance_map_) {
            recycler->stop();
        }
    }
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
    if (checker_) {
        checker_->stop();
    }
}

class InstanceRecycler::InvertedIndexIdCache {
public:
    InvertedIndexIdCache(std::string instance_id, std::shared_ptr<TxnKv> txn_kv)
            : instance_id_(std::move(instance_id)), txn_kv_(std::move(txn_kv)) {}

    // Return 0 if success, 1 if schema kv not found, negative for error
    int get(int64_t index_id, int32_t schema_version, std::vector<int64_t>& res) {
        {
            std::lock_guard lock(mtx_);
            if (schemas_without_inverted_index_.count({index_id, schema_version})) {
                return 0;
            }
            if (auto it = inverted_index_id_map_.find({index_id, schema_version});
                it != inverted_index_id_map_.end()) {
                res = it->second;
                return 0;
            }
        }
        // Get schema from kv
        // TODO(plat1ko): Single flight
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn, err=" << err;
            return -1;
        }
        auto schema_key = meta_schema_key({instance_id_, index_id, schema_version});
        ValueBuf val_buf;
        err = cloud::get(txn.get(), schema_key, &val_buf);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get schema, err=" << err;
            return static_cast<int>(err);
        }
        doris::TabletSchemaCloudPB schema;
        if (!parse_schema_value(val_buf, &schema)) {
            LOG(WARNING) << "malformed schema value, key=" << hex(schema_key);
            return -1;
        }
        if (schema.index_size() > 0) {
            res.reserve(schema.index_size());
            for (auto& i : schema.index()) {
                res.push_back(i.index_id());
            }
        }
        insert(index_id, schema_version, res);
        return 0;
    }

    // Empty `ids` means this schema has no inverted index
    void insert(int64_t index_id, int32_t schema_version, const std::vector<int64_t>& ids) {
        if (ids.empty()) {
            TEST_SYNC_POINT("InvertedIndexIdCache::insert1");
            std::lock_guard lock(mtx_);
            schemas_without_inverted_index_.emplace(index_id, schema_version);
        } else {
            TEST_SYNC_POINT("InvertedIndexIdCache::insert2");
            std::lock_guard lock(mtx_);
            inverted_index_id_map_.try_emplace({index_id, schema_version}, ids);
        }
    }

private:
    std::string instance_id_;
    std::shared_ptr<TxnKv> txn_kv_;

    std::mutex mtx_;
    using Key = std::pair<int64_t, int32_t>; // <index_id, schema_version>
    struct HashOfKey {
        size_t operator()(const Key& key) const {
            size_t seed = 0;
            seed = std::hash<int64_t> {}(key.first);
            seed = std::hash<int32_t> {}(key.second);
            return seed;
        }
    };
    // <index_id, schema_version> -> inverted_index_ids
    std::unordered_map<Key, std::vector<int64_t>, HashOfKey> inverted_index_id_map_;
    // Store <index_id, schema_version> of schema which doesn't have inverted index
    std::unordered_set<Key, HashOfKey> schemas_without_inverted_index_;
};

InstanceRecycler::InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance)
        : txn_kv_(std::move(txn_kv)),
          instance_id_(instance.instance_id()),
          instance_info_(instance),
          inverted_index_id_cache_(std::make_unique<InvertedIndexIdCache>(instance_id_, txn_kv_)) {}

InstanceRecycler::~InstanceRecycler() = default;

int InstanceRecycler::init_obj_store_accessors() {
    for (const auto& obj_info : instance_info_.obj_info()) {
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
            LOG(WARNING) << "failed to init s3 accessor. instance_id=" << instance_id_
                         << " resource_id=" << obj_info.id();
            return ret;
        }
#endif
        accessor_map_.emplace(obj_info.id(), std::move(accessor));
    }

    return 0;
}

int InstanceRecycler::init_storage_vault_accessors() {
    if (instance_info_.resource_ids().empty()) {
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

int InstanceRecycler::init() {
    int ret = init_obj_store_accessors();
    if (ret != 0) {
        return ret;
    }

    return init_storage_vault_accessors();
}

int InstanceRecycler::do_recycle() {
    TEST_SYNC_POINT("InstanceRecycler.do_recycle");
    if (instance_info_.status() == InstanceInfoPB::DELETED) {
        return recycle_deleted_instance();
    } else if (instance_info_.status() == InstanceInfoPB::NORMAL) {
        int ret = recycle_indexes();
        if (recycle_partitions() != 0) ret = -1;
        if (recycle_tmp_rowsets() != 0) ret = -1;
        if (recycle_rowsets() != 0) ret = -1;
        if (abort_timeout_txn() != 0) ret = -1;
        if (recycle_expired_txn_label() != 0) ret = -1;
        if (recycle_copy_jobs() != 0) ret = -1;
        if (recycle_stage() != 0) ret = -1;
        if (recycle_expired_stage_objects() != 0) ret = -1;
        if (recycle_versions() != 0) ret = -1;
        return ret;
    } else {
        LOG(WARNING) << "invalid instance status: " << instance_info_.status()
                     << " instance_id=" << instance_id_;
        return -1;
    }
}

int InstanceRecycler::recycle_deleted_instance() {
    LOG_INFO("begin to recycle deleted instance").tag("instance_id", instance_id_);

    int ret = 0;
    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << (ret == 0 ? "successfully" : "failed to")
                  << " recycle deleted instance, cost=" << cost
                  << "s, instance_id=" << instance_id_;
    });

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        ret = -1;
        return -1;
    }
    LOG(INFO) << "begin to delete all kv, instance_id=" << instance_id_;
    // delete kv before deleting objects to prevent the checker from misjudging data loss
    std::string start_txn_key = txn_key_prefix(instance_id_);
    std::string end_txn_key = txn_key_prefix(instance_id_ + '\x00');
    txn->remove(start_txn_key, end_txn_key);
    std::string start_version_key = version_key_prefix(instance_id_);
    std::string end_version_key = version_key_prefix(instance_id_ + '\x00');
    txn->remove(start_version_key, end_version_key);
    std::string start_meta_key = meta_key_prefix(instance_id_);
    std::string end_meta_key = meta_key_prefix(instance_id_ + '\x00');
    txn->remove(start_meta_key, end_meta_key);
    std::string start_recycle_key = recycle_key_prefix(instance_id_);
    std::string end_recycle_key = recycle_key_prefix(instance_id_ + '\x00');
    txn->remove(start_recycle_key, end_recycle_key);
    std::string start_stats_tablet_key = stats_tablet_key({instance_id_, 0, 0, 0, 0});
    std::string end_stats_tablet_key = stats_tablet_key({instance_id_, INT64_MAX, 0, 0, 0});
    txn->remove(start_stats_tablet_key, end_stats_tablet_key);
    std::string start_copy_key = copy_key_prefix(instance_id_);
    std::string end_copy_key = copy_key_prefix(instance_id_ + '\x00');
    txn->remove(start_copy_key, end_copy_key);
    // should not remove job key range, because we need to reserve job recycle kv
    // 0:instance_id  1:table_id  2:index_id  3:part_id  4:tablet_id
    std::string start_job_tablet_key = job_tablet_key({instance_id_, 0, 0, 0, 0});
    std::string end_job_tablet_key = job_tablet_key({instance_id_, INT64_MAX, 0, 0, 0});
    txn->remove(start_job_tablet_key, end_job_tablet_key);
    StorageVaultKeyInfo key_info0 {instance_id_, ""};
    StorageVaultKeyInfo key_info1 {instance_id_, "\xff"};
    std::string start_vault_key = storage_vault_key(key_info0);
    std::string end_vault_key = storage_vault_key(key_info1);
    txn->remove(start_vault_key, end_vault_key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to delete all kv, instance_id=" << instance_id_ << ", err=" << err;
        ret = -1;
    }

    for (auto& [_, accessor] : accessor_map_) {
        if (stopped()) {
            return ret;
        }

        LOG(INFO) << "begin to delete all objects in " << accessor->uri();
        int del_ret = accessor->delete_all();
        if (del_ret == 0) {
            LOG(INFO) << "successfully delete all objects in " << accessor->uri();
        } else if (del_ret != 1) { // no need to log, because S3Accessor has logged this error
            // If `del_ret == 1`, it can be considered that the object data has been recycled by cloud platform,
            // so the recycling has been successful.
            ret = -1;
        }
    }

    if (ret == 0) {
        // remove instance kv
        // ATTN: MUST ensure that cloud platform won't regenerate the same instance id
        err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            ret = -1;
            return ret;
        }
        std::string key;
        instance_key({instance_id_}, &key);
        txn->remove(key);
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to delete instance kv, instance_id=" << instance_id_
                         << " err=" << err;
            ret = -1;
        }
    }
    return ret;
}

int InstanceRecycler::recycle_indexes() {
    const std::string task_name = "recycle_indexes";
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleIndexKeyInfo index_key_info0 {instance_id_, 0};
    RecycleIndexKeyInfo index_key_info1 {instance_id_, INT64_MAX};
    std::string index_key0;
    std::string index_key1;
    recycle_index_key(index_key_info0, &index_key0);
    recycle_index_key(index_key_info1, &index_key1);

    LOG_INFO("begin to recycle indexes").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("recycle indexes finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    auto calc_expiration = [](const RecycleIndexPB& index) {
        int64_t expiration = index.expiration() > 0 ? index.expiration() : index.creation_time();
        int64_t retention_seconds = config::retention_seconds;
        if (index.state() == RecycleIndexPB::DROPPED) {
            retention_seconds =
                    std::min(config::dropped_index_retention_seconds, retention_seconds);
        }
        return expiration + retention_seconds;
    };

    // Elements in `index_keys` has the same lifetime as `it` in `scan_and_recycle`
    std::vector<std::string_view> index_keys;
    auto recycle_func = [&, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleIndexPB index_pb;
        if (!index_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle index value").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        if (current_time < calc_expiration(index_pb)) { // not expired
            return 0;
        }
        ++num_expired;
        // decode index_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "index" ${index_id} -> RecycleIndexPB
        auto index_id = std::get<int64_t>(std::get<0>(out[3]));
        LOG(INFO) << "begin to recycle index, instance_id=" << instance_id_
                  << " table_id=" << index_pb.table_id() << " index_id=" << index_id
                  << " state=" << RecycleIndexPB::State_Name(index_pb.state());
        // Change state to RECYCLING
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn").tag("err", err);
            return -1;
        }
        std::string val;
        err = txn->get(k, &val);
        if (err ==
            TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN, maybe recycled or committed, skip it
            LOG_INFO("index {} has been recycled or committed", index_id);
            return 0;
        }
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get kv").tag("key", hex(k)).tag("err", err);
            return -1;
        }
        index_pb.Clear();
        if (!index_pb.ParseFromString(val)) {
            LOG_WARNING("malformed recycle index value").tag("key", hex(k));
            return -1;
        }
        if (index_pb.state() != RecycleIndexPB::RECYCLING) {
            index_pb.set_state(RecycleIndexPB::RECYCLING);
            txn->put(k, index_pb.SerializeAsString());
            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to commit txn").tag("err", err);
                return -1;
            }
        }
        if (recycle_tablets(index_pb.table_id(), index_id) != 0) {
            LOG_WARNING("failed to recycle tablets under index")
                    .tag("table_id", index_pb.table_id())
                    .tag("instance_id", instance_id_)
                    .tag("index_id", index_id);
            return -1;
        }
        ++num_recycled;
        check_recycle_task(instance_id_, task_name, num_scanned, num_recycled, start_time);
        index_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&index_keys, this]() -> int {
        if (index_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                              [&](int*) { index_keys.clear(); });
        if (0 != txn_remove(txn_kv_.get(), index_keys)) {
            LOG(WARNING) << "failed to delete recycle index kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };

    return scan_and_recycle(index_key0, index_key1, std::move(recycle_func), std::move(loop_done));
}

int InstanceRecycler::recycle_partitions() {
    const std::string task_name = "recycle_partitions";
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecyclePartKeyInfo part_key_info0 {instance_id_, 0};
    RecyclePartKeyInfo part_key_info1 {instance_id_, INT64_MAX};
    std::string part_key0;
    std::string part_key1;
    recycle_partition_key(part_key_info0, &part_key0);
    recycle_partition_key(part_key_info1, &part_key1);

    LOG_INFO("begin to recycle partitions").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("recycle partitions finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    auto calc_expiration = [](const RecyclePartitionPB& partition) {
        int64_t expiration =
                partition.expiration() > 0 ? partition.expiration() : partition.creation_time();
        int64_t retention_seconds = config::retention_seconds;
        if (partition.state() == RecyclePartitionPB::DROPPED) {
            retention_seconds =
                    std::min(config::dropped_partition_retention_seconds, retention_seconds);
        }
        return expiration + retention_seconds;
    };

    // Elements in `partition_keys` has the same lifetime as `it` in `scan_and_recycle`
    std::vector<std::string_view> partition_keys;
    std::vector<std::string> partition_version_keys;
    auto recycle_func = [&, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecyclePartitionPB part_pb;
        if (!part_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle partition value").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        if (current_time < calc_expiration(part_pb)) { // not expired
            return 0;
        }
        ++num_expired;
        // decode partition_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "partition" ${partition_id} -> RecyclePartitionPB
        auto partition_id = std::get<int64_t>(std::get<0>(out[3]));
        LOG(INFO) << "begin to recycle partition, instance_id=" << instance_id_
                  << " table_id=" << part_pb.table_id() << " partition_id=" << partition_id
                  << " state=" << RecyclePartitionPB::State_Name(part_pb.state());
        // Change state to RECYCLING
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn").tag("err", err);
            return -1;
        }
        std::string val;
        err = txn->get(k, &val);
        if (err ==
            TxnErrorCode::TXN_KEY_NOT_FOUND) { // UNKNOWN, maybe recycled or committed, skip it
            LOG_INFO("partition {} has been recycled or committed", partition_id);
            return 0;
        }
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get kv");
            return -1;
        }
        part_pb.Clear();
        if (!part_pb.ParseFromString(val)) {
            LOG_WARNING("malformed recycle partition value").tag("key", hex(k));
            return -1;
        }
        // Partitions with PREPARED state MUST have no data
        bool is_empty_tablet = part_pb.state() == RecyclePartitionPB::PREPARED;
        if (part_pb.state() != RecyclePartitionPB::RECYCLING) {
            part_pb.set_state(RecyclePartitionPB::RECYCLING);
            txn->put(k, part_pb.SerializeAsString());
            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to commit txn: {}", err);
                return -1;
            }
        }
        int ret = 0;
        for (int64_t index_id : part_pb.index_id()) {
            if (recycle_tablets(part_pb.table_id(), index_id, partition_id, is_empty_tablet) != 0) {
                LOG_WARNING("failed to recycle tablets under partition")
                        .tag("table_id", part_pb.table_id())
                        .tag("instance_id", instance_id_)
                        .tag("index_id", index_id)
                        .tag("partition_id", partition_id);
                ret = -1;
            }
        }
        if (ret == 0) {
            ++num_recycled;
            check_recycle_task(instance_id_, task_name, num_scanned, num_recycled, start_time);
            partition_keys.push_back(k);
            if (part_pb.db_id() > 0) {
                partition_version_keys.push_back(partition_version_key(
                        {instance_id_, part_pb.db_id(), part_pb.table_id(), partition_id}));
            }
        }
        return ret;
    };

    auto loop_done = [&partition_keys, &partition_version_keys, this]() -> int {
        if (partition_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
            partition_keys.clear();
            partition_version_keys.clear();
        });
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to delete recycle partition kv, instance_id=" << instance_id_;
            return -1;
        }
        for (auto& k : partition_keys) {
            txn->remove(k);
        }
        for (auto& k : partition_version_keys) {
            txn->remove(k);
        }
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to delete recycle partition kv, instance_id=" << instance_id_
                         << " err=" << err;
            return -1;
        }
        return 0;
    };

    return scan_and_recycle(part_key0, part_key1, std::move(recycle_func), std::move(loop_done));
}

int InstanceRecycler::recycle_versions() {
    int num_scanned = 0;
    int num_recycled = 0;

    LOG_INFO("begin to recycle table and partition versions").tag("instance_id", instance_id_);

    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle table and partition versions finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    auto version_key_begin = partition_version_key({instance_id_, 0, 0, 0});
    auto version_key_end = partition_version_key({instance_id_, INT64_MAX, 0, 0});
    int64_t last_scanned_table_id = 0;
    bool is_recycled = false; // Is last scanned kv recycled
    auto recycle_func = [&num_scanned, &num_recycled, &last_scanned_table_id, &is_recycled, this](
                                std::string_view k, std::string_view) {
        ++num_scanned;
        auto k1 = k;
        k1.remove_prefix(1);
        // 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id}
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        DCHECK_EQ(out.size(), 6) << k;
        auto table_id = std::get<int64_t>(std::get<0>(out[4]));
        if (table_id == last_scanned_table_id) { // Already handle kvs of this table
            num_recycled += is_recycled;         // Version kv of this table has been recycled
            return 0;
        }
        last_scanned_table_id = table_id;
        is_recycled = false;
        auto tablet_key_begin = stats_tablet_key({instance_id_, table_id, 0, 0, 0});
        auto tablet_key_end = stats_tablet_key({instance_id_, table_id, INT64_MAX, 0, 0});
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            return -1;
        }
        std::unique_ptr<RangeGetIterator> iter;
        err = txn->get(tablet_key_begin, tablet_key_end, &iter, false, 1);
        if (err != TxnErrorCode::TXN_OK) {
            return -1;
        }
        if (iter->has_next()) { // Table is useful, should not recycle table and partition versions
            return 0;
        }
        auto db_id = std::get<int64_t>(std::get<0>(out[3]));
        // 1. Remove all partition version kvs of this table
        auto partition_version_key_begin =
                partition_version_key({instance_id_, db_id, table_id, 0});
        auto partition_version_key_end =
                partition_version_key({instance_id_, db_id, table_id, INT64_MAX});
        txn->remove(partition_version_key_begin, partition_version_key_end);
        LOG(WARNING) << "remove partition version kv, begin=" << hex(partition_version_key_begin)
                     << " end=" << hex(partition_version_key_end);
        // 2. Remove the table version kv of this table
        auto tbl_version_key = table_version_key({instance_id_, db_id, table_id});
        txn->remove(tbl_version_key);
        LOG(WARNING) << "remove table version kv " << hex(tbl_version_key);
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            return -1;
        }
        ++num_recycled;
        is_recycled = true;
        return 0;
    };

    return scan_and_recycle(version_key_begin, version_key_end, std::move(recycle_func));
}

int InstanceRecycler::recycle_tablets(int64_t table_id, int64_t index_id, int64_t partition_id,
                                      bool is_empty_tablet) {
    int num_scanned = 0;
    int num_recycled = 0;

    std::string tablet_key_begin, tablet_key_end;
    std::string stats_key_begin, stats_key_end;
    std::string job_key_begin, job_key_end;

    if (partition_id > 0) {
        // recycle tablets in a partition belonging to the index
        meta_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &tablet_key_end);
        stats_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &stats_key_begin);
        stats_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &stats_key_end);
        job_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &job_key_begin);
        job_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &job_key_end);
    } else {
        // recycle tablets in the index
        meta_tablet_key({instance_id_, table_id, index_id, 0, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &tablet_key_end);
        stats_tablet_key({instance_id_, table_id, index_id, 0, 0}, &stats_key_begin);
        stats_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &stats_key_end);
        job_tablet_key({instance_id_, table_id, index_id, 0, 0}, &job_key_begin);
        job_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &job_key_end);
    }

    LOG_INFO("begin to recycle tablets")
            .tag("table_id", table_id)
            .tag("index_id", index_id)
            .tag("partition_id", partition_id);

    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle tablets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("table_id", table_id)
                .tag("index_id", index_id)
                .tag("partition_id", partition_id)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    // Elements in `tablet_keys` has the same lifetime as `it` in `scan_and_recycle`
    std::vector<std::string_view> tablet_keys;
    std::vector<std::string> tablet_idx_keys;
    std::vector<std::string> init_rs_keys;
    bool use_range_remove = true;
    auto recycle_func = [&, is_empty_tablet, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        doris::TabletMetaCloudPB tablet_meta_pb;
        if (!tablet_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed tablet meta").tag("key", hex(k));
            use_range_remove = false;
            return -1;
        }
        int64_t tablet_id = tablet_meta_pb.tablet_id();
        tablet_idx_keys.push_back(meta_tablet_idx_key({instance_id_, tablet_id}));
        if (!is_empty_tablet) {
            if (recycle_tablet(tablet_id) != 0) {
                LOG_WARNING("failed to recycle tablet")
                        .tag("instance_id", instance_id_)
                        .tag("tablet_id", tablet_id);
                use_range_remove = false;
                return -1;
            }
        } else {
            // Empty tablet only has a [0-1] init rowset
            init_rs_keys.push_back(meta_rowset_key({instance_id_, tablet_id, 1}));
            DCHECK([&]() {
                std::unique_ptr<Transaction> txn;
                if (TxnErrorCode err = txn_kv_->create_txn(&txn); err != TxnErrorCode::TXN_OK) {
                    LOG_ERROR("failed to create txn").tag("err", err);
                    return false;
                }
                auto rs_key_begin = meta_rowset_key({instance_id_, tablet_id, 2});
                auto rs_key_end = meta_rowset_key({instance_id_, tablet_id, INT64_MAX});
                std::unique_ptr<RangeGetIterator> iter;
                if (TxnErrorCode err = txn->get(rs_key_begin, rs_key_end, &iter, true, 1);
                    err != TxnErrorCode::TXN_OK) {
                    LOG_ERROR("failed to get kv").tag("err", err);
                    return false;
                }
                if (iter->has_next()) {
                    LOG_ERROR("tablet is not empty").tag("tablet_id", tablet_id);
                    return false;
                }
                return true;
            }());
        }
        ++num_recycled;
        tablet_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&, this]() -> int {
        if (tablet_keys.empty() && tablet_idx_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
            tablet_keys.clear();
            tablet_idx_keys.clear();
            init_rs_keys.clear();
            use_range_remove = true;
        });
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to delete tablet meta kv, instance_id=" << instance_id_;
            return -1;
        }
        std::string tablet_key_end;
        if (!tablet_keys.empty()) {
            if (use_range_remove) {
                tablet_key_end = std::string(tablet_keys.back()) + '\x00';
                txn->remove(tablet_keys.front(), tablet_key_end);
            } else {
                for (auto k : tablet_keys) {
                    txn->remove(k);
                }
            }
        }
        for (auto& k : tablet_idx_keys) {
            txn->remove(k);
        }
        for (auto& k : init_rs_keys) {
            txn->remove(k);
        }
        if (TxnErrorCode err = txn->commit(); err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to delete kvs related to tablets, instance_id=" << instance_id_
                         << ", err=" << err;
            return -1;
        }
        return 0;
    };

    int ret = scan_and_recycle(tablet_key_begin, tablet_key_end, std::move(recycle_func),
                               std::move(loop_done));

    // directly remove tablet stats and tablet jobs of these dropped index or partition
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to delete tablet job or stats key, instance_id=" << instance_id_;
        return -1;
    }
    txn->remove(stats_key_begin, stats_key_end);
    LOG(WARNING) << "remove stats kv, begin=" << hex(stats_key_begin)
                 << " end=" << hex(stats_key_end);
    txn->remove(job_key_begin, job_key_end);
    LOG(WARNING) << "remove job kv, begin=" << hex(job_key_begin) << " end=" << hex(job_key_end);
    std::string schema_key_begin, schema_key_end;
    std::string schema_dict_key;
    if (partition_id <= 0) {
        // Delete schema kv of this index
        meta_schema_key({instance_id_, index_id, 0}, &schema_key_begin);
        meta_schema_key({instance_id_, index_id + 1, 0}, &schema_key_end);
        txn->remove(schema_key_begin, schema_key_end);
        LOG(WARNING) << "remove schema kv, begin=" << hex(schema_key_begin)
                     << " end=" << hex(schema_key_end);
        meta_schema_pb_dictionary_key({instance_id_, index_id}, &schema_dict_key);
        txn->remove(schema_dict_key);
        LOG(WARNING) << "remove schema dict kv, key=" << hex(schema_dict_key);
    }

    TxnErrorCode err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to delete tablet job or stats key, instance_id=" << instance_id_
                     << " err=" << err;
        return -1;
    }

    return ret;
}

int InstanceRecycler::delete_rowset_data(const doris::RowsetMetaCloudPB& rs_meta_pb) {
    int64_t num_segments = rs_meta_pb.num_segments();
    if (num_segments <= 0) return 0;
    if (!rs_meta_pb.has_tablet_schema()) {
        return delete_rowset_data(rs_meta_pb.resource_id(), rs_meta_pb.tablet_id(),
                                  rs_meta_pb.rowset_id_v2());
    }
    auto it = accessor_map_.find(rs_meta_pb.resource_id());
    if (it == accessor_map_.end()) {
        LOG_WARNING("instance has no such resource id")
                .tag("instance_id", instance_id_)
                .tag("resource_id", rs_meta_pb.resource_id());
        return -1;
    }
    auto& accessor = it->second;
    const auto& rowset_id = rs_meta_pb.rowset_id_v2();
    int64_t tablet_id = rs_meta_pb.tablet_id();
    // process inverted indexes
    std::vector<int64_t> index_ids;
    index_ids.reserve(rs_meta_pb.tablet_schema().index_size());
    for (auto& i : rs_meta_pb.tablet_schema().index()) {
        index_ids.push_back(i.index_id());
    }
    std::vector<std::string> file_paths;
    file_paths.reserve(num_segments * (1 + index_ids.size()));
    for (int64_t i = 0; i < num_segments; ++i) {
        file_paths.push_back(segment_path(tablet_id, rowset_id, i));
        for (int64_t index_id : index_ids) {
            file_paths.push_back(inverted_index_path(tablet_id, rowset_id, i, index_id));
        }
    }
    // TODO(AlexYue): seems could do do batch
    return accessor->delete_files(file_paths);
}

int InstanceRecycler::delete_rowset_data(const std::vector<doris::RowsetMetaCloudPB>& rowsets) {
    int ret = 0;
    // resource_id -> file_paths
    std::map<std::string, std::vector<std::string>> resource_file_paths;
    for (auto& rs : rowsets) {
        {
            std::lock_guard lock(recycled_tablets_mtx_);
            if (recycled_tablets_.count(rs.tablet_id())) {
                continue; // Rowset data has already been deleted
            }
        }

        auto it = accessor_map_.find(rs.resource_id());
        if (it == accessor_map_.end()) [[unlikely]] { // impossible
            LOG_WARNING("instance has no such resource id")
                    .tag("instance_id", instance_id_)
                    .tag("resource_id", rs.resource_id());
            ret = -1;
            continue;
        }

        auto& file_paths = resource_file_paths[rs.resource_id()];
        const auto& rowset_id = rs.rowset_id_v2();
        int64_t tablet_id = rs.tablet_id();
        int64_t num_segments = rs.num_segments();
        if (num_segments <= 0) continue;

        // process inverted indexes
        std::vector<int64_t> index_ids;
        if (rs.has_tablet_schema()) {
            index_ids.reserve(rs.tablet_schema().index().size());
            for (auto& index_pb : rs.tablet_schema().index()) {
                index_ids.push_back(index_pb.index_id());
            }
        } else { // Detached schema
            if (!rs.has_index_id() || !rs.has_schema_version()) {
                LOG(WARNING) << "rowset must have either schema or schema_version and index_id, "
                                "instance_id="
                             << instance_id_ << " tablet_id=" << tablet_id
                             << " rowset_id=" << rowset_id;
                ret = -1;
                continue;
            }
            int get_ret =
                    inverted_index_id_cache_->get(rs.index_id(), rs.schema_version(), index_ids);
            if (get_ret != 0) {
                if (get_ret == 1) { // Schema kv not found
                    // Check tablet existence
                    std::string tablet_idx_key, tablet_idx_val;
                    meta_tablet_idx_key({instance_id_, tablet_id}, &tablet_idx_key);
                    if (txn_get(txn_kv_.get(), tablet_idx_key, tablet_idx_val) == 1) {
                        // Tablet has been recycled, rowset data has already been deleted
                        std::lock_guard lock(recycled_tablets_mtx_);
                        recycled_tablets_.insert(tablet_id);
                        continue;
                    }
                }
                LOG(WARNING) << "failed to get schema kv for rowset, instance_id=" << instance_id_
                             << " tablet_id=" << tablet_id << " rowset_id=" << rowset_id;
                ret = -1;
                continue;
            }
        }
        for (int64_t i = 0; i < num_segments; ++i) {
            file_paths.push_back(segment_path(tablet_id, rowset_id, i));
            for (int64_t index_id : index_ids) {
                file_paths.push_back(inverted_index_path(tablet_id, rowset_id, i, index_id));
            }
        }
    }
    for (auto& [resource_id, file_paths] : resource_file_paths) {
        auto& accessor = accessor_map_[resource_id];
        DCHECK(accessor);
        if (accessor->delete_files(file_paths) != 0) {
            ret = -1;
        }
    }
    return ret;
}

int InstanceRecycler::delete_rowset_data(const std::string& resource_id, int64_t tablet_id,
                                         const std::string& rowset_id) {
    auto it = accessor_map_.find(resource_id);
    if (it == accessor_map_.end()) {
        LOG_WARNING("instance has no such resource id")
                .tag("instance_id", instance_id_)
                .tag("resource_id", resource_id);
        return -1;
    }
    auto& accessor = it->second;
    return accessor->delete_prefix(rowset_path_prefix(tablet_id, rowset_id));
}

int InstanceRecycler::recycle_tablet(int64_t tablet_id) {
    LOG_INFO("begin to recycle rowsets in a dropped tablet")
            .tag("instance_id", instance_id_)
            .tag("tablet_id", tablet_id);

    auto start_time = steady_clock::now();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id);
    });

    // delete all rowset kv in this tablet
    std::string rs_key0 = meta_rowset_key({instance_id_, tablet_id, 0});
    std::string rs_key1 = meta_rowset_key({instance_id_, tablet_id + 1, 0});
    std::string recyc_rs_key0 = recycle_rowset_key({instance_id_, tablet_id, ""});
    std::string recyc_rs_key1 = recycle_rowset_key({instance_id_, tablet_id + 1, ""});

    int ret = 0;
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to delete rowset kv of tablet " << tablet_id;
        ret = -1;
    }
    txn->remove(rs_key0, rs_key1);
    txn->remove(recyc_rs_key0, recyc_rs_key1);

    // remove delete bitmap for MoW table
    std::string pending_key = meta_pending_delete_bitmap_key({instance_id_, tablet_id});
    txn->remove(pending_key);
    std::string delete_bitmap_start = meta_delete_bitmap_key({instance_id_, tablet_id, "", 0, 0});
    std::string delete_bitmap_end = meta_delete_bitmap_key({instance_id_, tablet_id + 1, "", 0, 0});
    txn->remove(delete_bitmap_start, delete_bitmap_end);

    TxnErrorCode err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to delete rowset kv of tablet " << tablet_id << ", err=" << err;
        ret = -1;
    }

    // delete all rowset data in this tablet
    for (auto& [_, accessor] : accessor_map_) {
        if (accessor->delete_directory(tablet_path_prefix(tablet_id)) != 0) {
            LOG(WARNING) << "failed to delete rowset data of tablet " << tablet_id
                         << " s3_path=" << accessor->uri();
            ret = -1;
        }
    }

    if (ret == 0) {
        // All object files under tablet have been deleted
        std::lock_guard lock(recycled_tablets_mtx_);
        recycled_tablets_.insert(tablet_id);
    }

    return ret;
}

int InstanceRecycler::recycle_rowsets() {
    const std::string task_name = "recycle_rowsets";
    int num_scanned = 0;
    int num_expired = 0;
    int num_prepare = 0;
    size_t total_rowset_size = 0;
    size_t expired_rowset_size = 0;
    std::atomic_int num_recycled = 0;

    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id_, 0, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id_, INT64_MAX, ""};
    std::string recyc_rs_key0;
    std::string recyc_rs_key1;
    recycle_rowset_key(recyc_rs_key_info0, &recyc_rs_key0);
    recycle_rowset_key(recyc_rs_key_info1, &recyc_rs_key1);

    LOG_INFO("begin to recycle rowsets").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled)
                .tag("num_prepare", num_prepare)
                .tag("total_rowset_meta_size", total_rowset_size)
                .tag("expired_rowset_meta_size", expired_rowset_size);
    });

    std::vector<std::string> rowset_keys;
    std::vector<doris::RowsetMetaCloudPB> rowsets;

    // Store keys of rowset recycled by background workers
    std::mutex async_recycled_rowset_keys_mutex;
    std::vector<std::string> async_recycled_rowset_keys;
    auto worker_pool =
            std::make_unique<SimpleThreadPool>(config::instance_recycler_worker_pool_size);
    worker_pool->start();
    auto delete_rowset_data_by_prefix = [&](std::string key, const std::string& resource_id,
                                            int64_t tablet_id, const std::string& rowset_id) {
        // Try to delete rowset data in background thread
        int ret = worker_pool->submit_with_timeout(
                [&, resource_id, tablet_id, rowset_id, key]() mutable {
                    if (delete_rowset_data(resource_id, tablet_id, rowset_id) != 0) {
                        LOG(WARNING) << "failed to delete rowset data, key=" << hex(key);
                        return;
                    }
                    std::vector<std::string> keys;
                    {
                        std::lock_guard lock(async_recycled_rowset_keys_mutex);
                        async_recycled_rowset_keys.push_back(std::move(key));
                        if (async_recycled_rowset_keys.size() > 100) {
                            keys.swap(async_recycled_rowset_keys);
                        }
                    }
                    if (keys.empty()) return;
                    if (txn_remove(txn_kv_.get(), keys) != 0) {
                        LOG(WARNING) << "failed to delete recycle rowset kv, instance_id="
                                     << instance_id_;
                    } else {
                        num_recycled.fetch_add(keys.size(), std::memory_order_relaxed);
                        check_recycle_task(instance_id_, "recycle_rowsets", num_scanned,
                                           num_recycled, start_time);
                    }
                },
                0);
        if (ret == 0) return 0;
        // Submit task failed, delete rowset data in current thread
        if (delete_rowset_data(resource_id, tablet_id, rowset_id) != 0) {
            LOG(WARNING) << "failed to delete rowset data, key=" << hex(key);
            return -1;
        }
        rowset_keys.push_back(std::move(key));
        return 0;
    };

    auto calc_expiration = [](const RecycleRowsetPB& rs) {
        // RecycleRowsetPB created by compacted or dropped rowset has no expiration time, and will be recycled when exceed retention time
        int64_t expiration = rs.expiration() > 0 ? rs.expiration() : rs.creation_time();
        int64_t retention_seconds = config::retention_seconds;
        if (rs.type() == RecycleRowsetPB::COMPACT || rs.type() == RecycleRowsetPB::DROP) {
            retention_seconds =
                    std::min(config::compacted_rowset_retention_seconds, retention_seconds);
        }
        return expiration + retention_seconds;
    };

    auto handle_rowset_kv = [&](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        total_rowset_size += v.size();
        RecycleRowsetPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle rowset").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        if (current_time < calc_expiration(rowset)) { // not expired
            return 0;
        }
        ++num_expired;
        expired_rowset_size += v.size();
        if (!rowset.has_type()) {                         // old version `RecycleRowsetPB`
            if (!rowset.has_resource_id()) [[unlikely]] { // impossible
                // in old version, keep this key-value pair and it needs to be checked manually
                LOG_WARNING("rowset meta has empty resource id").tag("key", hex(k));
                return -1;
            }
            if (rowset.resource_id().empty()) [[unlikely]] {
                // old version `RecycleRowsetPB` may has empty resource_id, just remove the kv.
                LOG(INFO) << "delete the recycle rowset kv that has empty resource_id, key="
                          << hex(k) << " value=" << proto_to_json(rowset);
                rowset_keys.push_back(std::string(k));
                return -1;
            }
            // decode rowset_id
            auto k1 = k;
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
            const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
            LOG(INFO) << "delete rowset data, instance_id=" << instance_id_
                      << " tablet_id=" << rowset.tablet_id() << " rowset_id=" << rowset_id;
            if (delete_rowset_data_by_prefix(std::string(k), rowset.resource_id(),
                                             rowset.tablet_id(), rowset_id) != 0) {
                return -1;
            }
            return 0;
        }
        // TODO(plat1ko): check rowset not referenced
        auto rowset_meta = rowset.mutable_rowset_meta();
        if (!rowset_meta->has_resource_id()) [[unlikely]] { // impossible
            if (rowset.type() != RecycleRowsetPB::PREPARE && rowset_meta->num_segments() == 0) {
                LOG_INFO("recycle rowset that has empty resource id");
            } else {
                // other situations, keep this key-value pair and it needs to be checked manually
                LOG_WARNING("rowset meta has empty resource id").tag("key", hex(k));
                return -1;
            }
        }
        LOG(INFO) << "delete rowset data, instance_id=" << instance_id_
                  << " tablet_id=" << rowset_meta->tablet_id()
                  << " rowset_id=" << rowset_meta->rowset_id_v2() << " version=["
                  << rowset_meta->start_version() << '-' << rowset_meta->end_version()
                  << "] txn_id=" << rowset_meta->txn_id()
                  << " type=" << RecycleRowsetPB_Type_Name(rowset.type())
                  << " rowset_meta_size=" << v.size() << " creation_time"
                  << rowset_meta->creation_time();
        if (rowset.type() == RecycleRowsetPB::PREPARE) {
            // unable to calculate file path, can only be deleted by rowset id prefix
            num_prepare += 1;
            if (delete_rowset_data_by_prefix(std::string(k), rowset_meta->resource_id(),
                                             rowset_meta->tablet_id(),
                                             rowset_meta->rowset_id_v2()) != 0) {
                return -1;
            }
        } else {
            rowset_keys.push_back(std::string(k));
            if (rowset_meta->num_segments() > 0) { // Skip empty rowset
                rowsets.push_back(std::move(*rowset_meta));
            }
        }
        return 0;
    };

    auto loop_done = [&]() -> int {
        std::vector<std::string> rowset_keys_to_delete;
        std::vector<doris::RowsetMetaCloudPB> rowsets_to_delete;
        rowset_keys_to_delete.swap(rowset_keys);
        rowsets_to_delete.swap(rowsets);
        worker_pool->submit([&, rowset_keys_to_delete = std::move(rowset_keys_to_delete),
                             rowsets_to_delete = std::move(rowsets_to_delete)]() {
            if (delete_rowset_data(rowsets_to_delete) != 0) {
                LOG(WARNING) << "failed to delete rowset data, instance_id=" << instance_id_;
                return;
            }
            if (txn_remove(txn_kv_.get(), rowset_keys_to_delete) != 0) {
                LOG(WARNING) << "failed to delete recycle rowset kv, instance_id=" << instance_id_;
                return;
            }
            num_recycled.fetch_add(rowset_keys_to_delete.size(), std::memory_order_relaxed);
        });
        return 0;
    };

    int ret = scan_and_recycle(recyc_rs_key0, recyc_rs_key1, std::move(handle_rowset_kv),
                               std::move(loop_done));
    worker_pool->stop();

    if (!async_recycled_rowset_keys.empty()) {
        if (txn_remove(txn_kv_.get(), async_recycled_rowset_keys) != 0) {
            LOG(WARNING) << "failed to delete recycle rowset kv, instance_id=" << instance_id_;
            return -1;
        } else {
            num_recycled.fetch_add(async_recycled_rowset_keys.size(), std::memory_order_relaxed);
        }
    }
    return ret;
}

int InstanceRecycler::recycle_tmp_rowsets() {
    const std::string task_name = "recycle_tmp_rowsets";
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;
    size_t expired_rowset_size = 0;
    size_t total_rowset_size = 0;

    MetaRowsetTmpKeyInfo tmp_rs_key_info0 {instance_id_, 0, 0};
    MetaRowsetTmpKeyInfo tmp_rs_key_info1 {instance_id_, INT64_MAX, 0};
    std::string tmp_rs_key0;
    std::string tmp_rs_key1;
    meta_rowset_tmp_key(tmp_rs_key_info0, &tmp_rs_key0);
    meta_rowset_tmp_key(tmp_rs_key_info1, &tmp_rs_key1);

    LOG_INFO("begin to recycle tmp rowsets").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("recycle tmp rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled)
                .tag("total_rowset_meta_size", total_rowset_size)
                .tag("expired_rowset_meta_size", expired_rowset_size);
    });

    // Elements in `tmp_rowset_keys` has the same lifetime as `it`
    std::vector<std::string_view> tmp_rowset_keys;
    std::vector<doris::RowsetMetaCloudPB> tmp_rowsets;

    auto handle_rowset_kv = [&num_scanned, &num_expired, &tmp_rowset_keys, &tmp_rowsets,
                             &expired_rowset_size, &total_rowset_size,
                             this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        total_rowset_size += v.size();
        doris::RowsetMetaCloudPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed rowset meta").tag("key", hex(k));
            return -1;
        }
        int64_t current_time = ::time(nullptr);
        // ATTN: `txn_expiration` should > 0, however we use `creation_time` + a large `retention_time` (> 1 day in production environment)
        //  when `txn_expiration` <= 0 in some unexpected situation (usually when there are bugs). This is usually safe, coz loading
        //  duration or timeout always < `retention_time` in practice.
        int64_t expiration =
                rowset.txn_expiration() > 0 ? rowset.txn_expiration() : rowset.creation_time();
        if (current_time < expiration + config::retention_seconds) {
            // not expired
            return 0;
        }
        ++num_expired;
        expired_rowset_size += v.size();
        if (!rowset.has_resource_id()) {
            if (rowset.num_segments() > 0) [[unlikely]] { // impossible
                LOG_WARNING("rowset meta has empty resource id").tag("key", k);
                return -1;
            }
            // might be a delete pred rowset
            tmp_rowset_keys.push_back(k);
            return 0;
        }
        // TODO(plat1ko): check rowset not referenced
        LOG(INFO) << "delete rowset data, instance_id=" << instance_id_
                  << " tablet_id=" << rowset.tablet_id() << " rowset_id=" << rowset.rowset_id_v2()
                  << " version=[" << rowset.start_version() << '-' << rowset.end_version()
                  << "] txn_id=" << rowset.txn_id() << " rowset_meta_size=" << v.size()
                  << " creation_time" << rowset.creation_time();
        tmp_rowset_keys.push_back(k);
        if (rowset.num_segments() > 0) { // Skip empty rowset
            tmp_rowsets.push_back(std::move(rowset));
        }
        return 0;
    };

    auto loop_done = [&tmp_rowset_keys, &tmp_rowsets, &num_recycled, this]() -> int {
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
            tmp_rowset_keys.clear();
            tmp_rowsets.clear();
        });
        if (delete_rowset_data(tmp_rowsets) != 0) {
            LOG(WARNING) << "failed to delete tmp rowset data, instance_id=" << instance_id_;
            return -1;
        }
        if (txn_remove(txn_kv_.get(), tmp_rowset_keys) != 0) {
            LOG(WARNING) << "failed to delete tmp rowset kv, instance_id=" << instance_id_;
            return -1;
        }
        num_recycled += tmp_rowset_keys.size();
        return 0;
    };

    return scan_and_recycle(tmp_rs_key0, tmp_rs_key1, std::move(handle_rowset_kv),
                            std::move(loop_done));
}

int InstanceRecycler::scan_and_recycle(
        std::string begin, std::string_view end,
        std::function<int(std::string_view k, std::string_view v)> recycle_func,
        std::function<int()> loop_done) {
    int ret = 0;
    std::unique_ptr<RangeGetIterator> it;
    do {
        int get_ret = txn_get(txn_kv_.get(), begin, end, it);
        if (get_ret != 0) {
            LOG(WARNING) << "failed to get kv, key=" << begin << " ret=" << get_ret;
            return -1;
        }
        VLOG_DEBUG << "fetch " << it->size() << " kv";
        if (!it->has_next()) {
            VLOG_DEBUG << "no keys in the given range, begin=" << hex(begin) << " end=" << hex(end);
            break;
        }
        while (it->has_next()) {
            // recycle corresponding resources
            auto [k, v] = it->next();
            if (!it->has_next()) {
                begin = k;
                VLOG_DEBUG << "iterator has no more kvs. key=" << hex(k);
            }
            if (recycle_func(k, v) != 0) ret = -1;
        }
        begin.push_back('\x00'); // Update to next smallest key for iteration
        if (loop_done) {
            if (loop_done() != 0) ret = -1;
        }
    } while (it->more() && !stopped());
    return ret;
}

int InstanceRecycler::abort_timeout_txn() {
    const std::string task_name = "abort_timeout_txn";
    int num_scanned = 0;
    int num_timeout = 0;
    int num_abort = 0;

    TxnRunningKeyInfo txn_running_key_info0 {instance_id_, 0, 0};
    TxnRunningKeyInfo txn_running_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_txn_running_key;
    std::string end_txn_running_key;
    txn_running_key(txn_running_key_info0, &begin_txn_running_key);
    txn_running_key(txn_running_key_info1, &end_txn_running_key);

    LOG_INFO("begin to abort timeout txn").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("end to abort timeout txn, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_timeout", num_timeout)
                .tag("num_abort", num_abort);
    });

    int64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto handle_txn_running_kv = [&num_scanned, &num_timeout, &num_abort, &current_time, this](
                                         std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        TxnRunningPB txn_running_pb;
        if (!txn_running_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed txn_running_pb").tag("key", hex(k));
            return -1;
        }
        if (txn_running_pb.timeout_time() > current_time) {
            return 0;
        }
        ++num_timeout;

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_ERROR("failed to create txn err={}", err).tag("key", hex(k));
            return -1;
        }
        std::string_view k1 = k;
        //TxnRunningKeyInfo 0:instance_id  1:db_id  2:txn_id
        k1.remove_prefix(1); // Remove key space
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        if (decode_key(&k1, &out) != 0) {
            LOG_ERROR("failed to decode key").tag("key", hex(k));
            return -1;
        }
        int64_t db_id = std::get<int64_t>(std::get<0>(out[3]));
        int64_t txn_id = std::get<int64_t>(std::get<0>(out[4]));
        VLOG_DEBUG << "instance_id=" << instance_id_ << " db_id=" << db_id << " txn_id=" << txn_id;
        // Update txn_info
        std::string txn_inf_key, txn_inf_val;
        txn_info_key({instance_id_, db_id, txn_id}, &txn_inf_key);
        err = txn->get(txn_inf_key, &txn_inf_val);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get txn info err={}", err).tag("key", hex(txn_inf_key));
            return -1;
        }
        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(txn_inf_val)) {
            LOG_WARNING("failed to parse txn info").tag("key", hex(k));
            return -1;
        }
        txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
        txn_info.set_finish_time(current_time);
        txn_info.set_reason("timeout");
        VLOG_DEBUG << "txn_info=" << txn_info.DebugString();
        txn_inf_val.clear();
        if (!txn_info.SerializeToString(&txn_inf_val)) {
            LOG_WARNING("failed to serialize txn info").tag("key", hex(k));
            return -1;
        }
        txn->put(txn_inf_key, txn_inf_val);
        VLOG_DEBUG << "txn->put, txn_inf_key=" << hex(txn_inf_key);
        // Put recycle txn key
        std::string recyc_txn_key, recyc_txn_val;
        recycle_txn_key({instance_id_, db_id, txn_id}, &recyc_txn_key);
        RecycleTxnPB recycle_txn_pb;
        recycle_txn_pb.set_creation_time(current_time);
        recycle_txn_pb.set_label(txn_info.label());
        if (!recycle_txn_pb.SerializeToString(&recyc_txn_val)) {
            LOG_WARNING("failed to serialize txn recycle info")
                    .tag("key", hex(k))
                    .tag("db_id", db_id)
                    .tag("txn_id", txn_id);
            return -1;
        }
        txn->put(recyc_txn_key, recyc_txn_val);
        // Remove txn running key
        txn->remove(k);
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to commit txn err={}", err)
                    .tag("key", hex(k))
                    .tag("db_id", db_id)
                    .tag("txn_id", txn_id);
            return -1;
        }
        ++num_abort;
        return 0;
    };

    return scan_and_recycle(begin_txn_running_key, end_txn_running_key,
                            std::move(handle_txn_running_kv));
}

int InstanceRecycler::recycle_expired_txn_label() {
    const std::string task_name = "recycle_expired_txn_label";
    int num_scanned = 0;
    int num_expired = 0;
    int num_recycled = 0;

    RecycleTxnKeyInfo recycle_txn_key_info0 {instance_id_, 0, 0};
    RecycleTxnKeyInfo recycle_txn_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_recycle_txn_key;
    std::string end_recycle_txn_key;
    recycle_txn_key(recycle_txn_key_info0, &begin_recycle_txn_key);
    recycle_txn_key(recycle_txn_key_info1, &end_recycle_txn_key);

    LOG_INFO("begin to recycle expire txn").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("end to recycle expired txn, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    int64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto handle_recycle_txn_kv = [&num_scanned, &num_expired, &num_recycled, &current_time, this](
                                         std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleTxnPB recycle_txn_pb;
        if (!recycle_txn_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed txn_running_pb").tag("key", hex(k));
            return -1;
        }
        if ((recycle_txn_pb.has_immediate() && recycle_txn_pb.immediate()) ||
            (recycle_txn_pb.creation_time() + config::label_keep_max_second * 1000L <=
             current_time)) {
            LOG_INFO("found recycle txn").tag("key", hex(k));
            num_expired++;
        } else {
            return 0;
        }
        std::string_view k1 = k;
        //RecycleTxnKeyInfo 0:instance_id  1:db_id  2:txn_id
        k1.remove_prefix(1); // Remove key space
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        int ret = decode_key(&k1, &out);
        if (ret != 0) {
            LOG_ERROR("failed to decode key, ret={}", ret).tag("key", hex(k));
            return -1;
        }
        int64_t db_id = std::get<int64_t>(std::get<0>(out[3]));
        int64_t txn_id = std::get<int64_t>(std::get<0>(out[4]));
        VLOG_DEBUG << "instance_id=" << instance_id_ << " db_id=" << db_id << " txn_id=" << txn_id;
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_ERROR("failed to create txn err={}", err).tag("key", hex(k));
            return -1;
        }
        // Remove txn index kv
        auto index_key = txn_index_key({instance_id_, txn_id});
        txn->remove(index_key);
        // Remove txn info kv
        std::string info_key, info_val;
        txn_info_key({instance_id_, db_id, txn_id}, &info_key);
        err = txn->get(info_key, &info_val);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get txn info err={}", err).tag("key", hex(info_key));
            return -1;
        }
        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(info_val)) {
            LOG_WARNING("failed to parse txn info").tag("key", hex(info_key));
            return -1;
        }
        txn->remove(info_key);
        // Remove sub txn index kvs
        std::vector<std::string> sub_txn_index_keys;
        for (auto sub_txn_id : txn_info.sub_txn_ids()) {
            auto sub_txn_index_key = txn_index_key({instance_id_, sub_txn_id});
            sub_txn_index_keys.push_back(sub_txn_index_key);
        }
        for (auto& sub_txn_index_key : sub_txn_index_keys) {
            txn->remove(sub_txn_index_key);
        }
        // Update txn label
        std::string label_key, label_val;
        txn_label_key({instance_id_, db_id, txn_info.label()}, &label_key);
        err = txn->get(label_key, &label_val);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get txn label, txn_id=" << txn_id << " key=" << label_key
                         << " err=" << err;
            return -1;
        }
        TxnLabelPB txn_label;
        if (!txn_label.ParseFromArray(label_val.data(), label_val.size() - VERSION_STAMP_LEN)) {
            LOG_WARNING("failed to parse txn label").tag("key", hex(label_key));
            return -1;
        }
        auto it = std::find(txn_label.txn_ids().begin(), txn_label.txn_ids().end(), txn_id);
        if (it != txn_label.txn_ids().end()) {
            txn_label.mutable_txn_ids()->erase(it);
        }
        if (txn_label.txn_ids().empty()) {
            txn->remove(label_key);
        } else {
            if (!txn_label.SerializeToString(&label_val)) {
                LOG(WARNING) << "failed to serialize txn label, key=" << hex(label_key);
                return -1;
            }
            txn->atomic_set_ver_value(label_key, label_val);
        }
        // Remove recycle txn kv
        txn->remove(k);
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to delete expired txn, err=" << err << " key=" << hex(k);
            return -1;
        }
        ++num_recycled;
        LOG(INFO) << "recycle expired txn, key=" << hex(k);
        return 0;
    };

    return scan_and_recycle(begin_recycle_txn_key, end_recycle_txn_key,
                            std::move(handle_recycle_txn_kv));
}

struct CopyJobIdTuple {
    std::string instance_id;
    std::string stage_id;
    long table_id;
    std::string copy_id;
    std::string stage_path;
};
struct BatchObjStoreAccessor {
    BatchObjStoreAccessor(std::shared_ptr<StorageVaultAccessor> accessor, uint64_t& batch_count,
                          TxnKv* txn_kv)
            : accessor_(std::move(accessor)), batch_count_(batch_count), txn_kv_(txn_kv) {};
    ~BatchObjStoreAccessor() {
        if (!paths_.empty()) {
            consume();
        }
    }

    /**
    * To implicitely do batch work and submit the batch delete task to s3
    * The s3 delete opreations would be done in batches, and then delete CopyJobPB key one by one
    *
    * @param copy_job The protubuf struct consists of the copy job files.
    * @param key The copy job's key on fdb, the key is originally occupied by fdb range iterator, to make sure
    *            it would last until we finish the delete task, here we need pass one string value
    * @param cope_job_id_tuple One tuple {log_trace instance_id, stage_id, table_id, query_id, stage_path} to print log
    */
    void add(CopyJobPB copy_job, std::string key, const CopyJobIdTuple cope_job_id_tuple) {
        auto& [instance_id, stage_id, table_id, copy_id, path] = cope_job_id_tuple;
        auto& file_keys = copy_file_keys_[key];
        file_keys.log_trace =
                fmt::format("instance_id={}, stage_id={}, table_id={}, query_id={}, path={}",
                            instance_id, stage_id, table_id, copy_id, path);
        std::string_view log_trace = file_keys.log_trace;
        for (const auto& file : copy_job.object_files()) {
            auto relative_path = file.relative_path();
            paths_.push_back(relative_path);
            file_keys.keys.push_back(copy_file_key(
                    {instance_id, stage_id, table_id, file.relative_path(), file.etag()}));
            LOG_INFO(log_trace)
                    .tag("relative_path", relative_path)
                    .tag("batch_count", batch_count_);
        }
        LOG_INFO(log_trace)
                .tag("objects_num", copy_job.object_files().size())
                .tag("batch_count", batch_count_);
        // TODO(AlexYue): If the size is 1001, it would be one delete with 1000 objects and one delete request with only one object(**ATTN**: DOESN'T
        // recommend using delete objects when objects num is less than 10)
        if (paths_.size() < 1000) {
            return;
        }
        consume();
    }

private:
    void consume() {
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [this](int*) {
            paths_.clear();
            copy_file_keys_.clear();
            batch_count_++;
        });
        LOG_INFO("begin to delete {} internal stage objects in batch {}", paths_.size(),
                 batch_count_);
        StopWatch sw;
        // TODO(yuejing): 在accessor的delete_objets的实现里可以考虑如果_paths数量不超过10个的话，就直接发10个delete objection operation而不是发post
        if (0 != accessor_->delete_files(paths_)) {
            LOG_WARNING("failed to delete {} internal stage objects in batch {} and it takes {} us",
                        paths_.size(), batch_count_, sw.elapsed_us());
            return;
        }
        LOG_INFO("succeed to delete {} internal stage objects in batch {} and it takes {} us",
                 paths_.size(), batch_count_, sw.elapsed_us());
        // delete fdb's keys
        for (auto& file_keys : copy_file_keys_) {
            auto& [log_trace, keys] = file_keys.second;
            std::unique_ptr<Transaction> txn;
            if (txn_kv_->create_txn(&txn) != cloud::TxnErrorCode::TXN_OK) {
                LOG(WARNING) << "failed to create txn";
                continue;
            }
            // FIXME: We have already limited the file num and file meta size when selecting file in FE.
            // And if too many copy files, begin_copy failed commit too. So here the copy file keys are
            // limited, should not cause the txn commit failed.
            for (const auto& key : keys) {
                txn->remove(key);
                LOG_INFO("remove copy_file_key={}, {}", hex(key), log_trace);
            }
            txn->remove(file_keys.first);
            if (auto ret = txn->commit(); ret != cloud::TxnErrorCode::TXN_OK) {
                LOG(WARNING) << "failed to commit txn ret is " << ret;
                continue;
            }
        }
    }
    std::shared_ptr<StorageVaultAccessor> accessor_;
    // the path of the s3 files to be deleted
    std::vector<std::string> paths_;
    struct CopyFiles {
        std::string log_trace;
        std::vector<std::string> keys;
    };
    // pair<std::string, std::vector<std::string>>
    // first: instance_id_ stage_id table_id query_id
    // second: keys to be deleted
    // <fdb key, <{instance_id_ stage_id table_id query_id}, file keys to be deleted>>
    std::unordered_map<std::string, CopyFiles> copy_file_keys_;
    // used to distinguish different batch tasks, the task log consists of thread ID and batch number
    // which can together uniquely identifies different tasks for tracing log
    uint64_t& batch_count_;
    TxnKv* txn_kv_;
};

int InstanceRecycler::recycle_copy_jobs() {
    int num_scanned = 0;
    int num_finished = 0;
    int num_expired = 0;
    int num_recycled = 0;
    // Used for INTERNAL stage's copy jobs to tag each batch for log trace
    uint64_t batch_count = 0;
    const std::string task_name = "recycle_copy_jobs";

    LOG_INFO("begin to recycle copy jobs").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("recycle copy jobs finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_finished", num_finished)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    });

    CopyJobKeyInfo key_info0 {instance_id_, "", 0, "", 0};
    CopyJobKeyInfo key_info1 {instance_id_, "\xff", 0, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);
    std::unordered_map<std::string, std::shared_ptr<BatchObjStoreAccessor>> stage_accessor_map;
    auto recycle_func = [&start_time, &num_scanned, &num_finished, &num_expired, &num_recycled,
                         &batch_count, &stage_accessor_map, &task_name,
                         this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        CopyJobPB copy_job;
        if (!copy_job.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed copy job").tag("key", hex(k));
            return -1;
        }

        // decode copy job key
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "copy" ${instance_id} "job" ${stage_id} ${table_id} ${copy_id} ${group_id}
        // -> CopyJobPB
        const auto& stage_id = std::get<std::string>(std::get<0>(out[3]));
        const auto& table_id = std::get<int64_t>(std::get<0>(out[4]));
        const auto& copy_id = std::get<std::string>(std::get<0>(out[5]));

        bool check_storage = true;
        if (copy_job.job_status() == CopyJobPB::FINISH) {
            ++num_finished;

            if (copy_job.stage_type() == StagePB::INTERNAL) {
                auto it = stage_accessor_map.find(stage_id);
                std::shared_ptr<BatchObjStoreAccessor> accessor;
                std::string_view path;
                if (it != stage_accessor_map.end()) {
                    accessor = it->second;
                } else {
                    std::shared_ptr<StorageVaultAccessor> inner_accessor;
                    auto ret = init_copy_job_accessor(stage_id, copy_job.stage_type(),
                                                      &inner_accessor);
                    if (ret < 0) { // error
                        LOG_WARNING("Failed to init_copy_job_accessor due to error code {}", ret);
                        return -1;
                    } else if (ret == 0) {
                        path = inner_accessor->uri();
                        accessor = std::make_shared<BatchObjStoreAccessor>(
                                inner_accessor, batch_count, txn_kv_.get());
                        stage_accessor_map.emplace(stage_id, accessor);
                    } else { // stage not found, skip check storage
                        check_storage = false;
                    }
                }
                if (check_storage) {
                    // TODO delete objects with key and etag is not supported
                    accessor->add(std::move(copy_job), std::string(k),
                                  {instance_id_, stage_id, table_id, copy_id, std::string(path)});
                    return 0;
                }
            } else if (copy_job.stage_type() == StagePB::EXTERNAL) {
                int64_t current_time =
                        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                if (copy_job.finish_time_ms() > 0) {
                    if (current_time <
                        copy_job.finish_time_ms() + config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                } else {
                    // For compatibility, copy job does not contain finish time before 2.2.2, use start time
                    if (current_time <
                        copy_job.start_time_ms() + config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                }
            }
        } else if (copy_job.job_status() == CopyJobPB::LOADING) {
            int64_t current_time =
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            // if copy job is timeout: delete all copy file kvs and copy job kv
            if (current_time <= copy_job.timeout_time_ms()) {
                return 0;
            }
            ++num_expired;
        }

        // delete all copy files
        std::vector<std::string> copy_file_keys;
        for (auto& file : copy_job.object_files()) {
            copy_file_keys.push_back(copy_file_key(
                    {instance_id_, stage_id, table_id, file.relative_path(), file.etag()}));
        }
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }
        // FIXME: We have already limited the file num and file meta size when selecting file in FE.
        // And if too many copy files, begin_copy failed commit too. So here the copy file keys are
        // limited, should not cause the txn commit failed.
        for (const auto& key : copy_file_keys) {
            txn->remove(key);
            LOG(INFO) << "remove copy_file_key=" << hex(key) << ", instance_id=" << instance_id_
                      << ", stage_id=" << stage_id << ", table_id=" << table_id
                      << ", query_id=" << copy_id;
        }
        txn->remove(k);
        TxnErrorCode err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to commit txn, err=" << err;
            return -1;
        }

        ++num_recycled;
        check_recycle_task(instance_id_, task_name, num_scanned, num_recycled, start_time);
        return 0;
    };

    return scan_and_recycle(key0, key1, std::move(recycle_func));
}

int InstanceRecycler::init_copy_job_accessor(const std::string& stage_id,
                                             const StagePB::StageType& stage_type,
                                             std::shared_ptr<StorageVaultAccessor>* accessor) {
#ifdef UNIT_TEST
    // In unit test, external use the same accessor as the internal stage
    auto it = accessor_map_.find(stage_id);
    if (it != accessor_map_.end()) {
        *accessor = it->second;
    } else {
        std::cout << "UT can not find accessor with stage_id: " << stage_id << std::endl;
        return 1;
    }
#else
    // init s3 accessor and add to accessor map
    auto stage_it =
            std::find_if(instance_info_.stages().begin(), instance_info_.stages().end(),
                         [&stage_id](auto&& stage) { return stage.stage_id() == stage_id; });

    if (stage_it == instance_info_.stages().end()) {
        LOG(INFO) << "Recycle nonexisted stage copy jobs. instance_id=" << instance_id_
                  << ", stage_id=" << stage_id << ", stage_type=" << stage_type;
        return 1;
    }

    const auto& object_store_info = stage_it->obj_info();
    auto stage_access_type = stage_it->has_access_type() ? stage_it->access_type() : StagePB::AKSK;

    S3Conf s3_conf;
    if (stage_type == StagePB::EXTERNAL) {
        if (stage_access_type == StagePB::AKSK) {
            auto conf = S3Conf::from_obj_store_info(object_store_info);
            if (!conf) {
                return -1;
            }

            s3_conf = std::move(*conf);
        } else if (stage_access_type == StagePB::BUCKET_ACL) {
            auto conf = S3Conf::from_obj_store_info(object_store_info, true /* skip_aksk */);
            if (!conf) {
                return -1;
            }

            s3_conf = std::move(*conf);
            if (instance_info_.ram_user().has_encryption_info()) {
                AkSkPair plain_ak_sk_pair;
                int ret = decrypt_ak_sk_helper(
                        instance_info_.ram_user().ak(), instance_info_.ram_user().sk(),
                        instance_info_.ram_user().encryption_info(), &plain_ak_sk_pair);
                if (ret != 0) {
                    LOG(WARNING) << "fail to decrypt ak sk. instance_id: " << instance_id_
                                 << " ram_user: " << proto_to_json(instance_info_.ram_user());
                    return -1;
                }
                s3_conf.ak = std::move(plain_ak_sk_pair.first);
                s3_conf.sk = std::move(plain_ak_sk_pair.second);
            } else {
                s3_conf.ak = instance_info_.ram_user().ak();
                s3_conf.sk = instance_info_.ram_user().sk();
            }
        } else {
            LOG(INFO) << "Unsupported stage access type=" << stage_access_type
                      << ", instance_id=" << instance_id_ << ", stage_id=" << stage_id;
            return -1;
        }
    } else if (stage_type == StagePB::INTERNAL) {
        int idx = stoi(object_store_info.id());
        if (idx > instance_info_.obj_info().size() || idx < 1) {
            LOG(WARNING) << "invalid idx: " << idx;
            return -1;
        }

        const auto& old_obj = instance_info_.obj_info()[idx - 1];
        auto conf = S3Conf::from_obj_store_info(old_obj);
        if (!conf) {
            return -1;
        }

        s3_conf = std::move(*conf);
        s3_conf.prefix = object_store_info.prefix();
    } else {
        LOG(WARNING) << "unknown stage type " << stage_type;
        return -1;
    }

    std::shared_ptr<S3Accessor> s3_accessor;
    int ret = S3Accessor::create(std::move(s3_conf), &s3_accessor);
    if (ret != 0) {
        LOG(WARNING) << "failed to init s3 accessor ret=" << ret;
        return -1;
    }

    *accessor = std::move(s3_accessor);
#endif
    return 0;
}

int InstanceRecycler::recycle_stage() {
    int num_scanned = 0;
    int num_recycled = 0;
    const std::string task_name = "recycle_stage";

    LOG_INFO("begin to recycle stage").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("recycle stage, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    });

    RecycleStageKeyInfo key_info0 {instance_id_, ""};
    RecycleStageKeyInfo key_info1 {instance_id_, "\xff"};
    std::string key0;
    std::string key1;
    recycle_stage_key(key_info0, &key0);
    recycle_stage_key(key_info1, &key1);

    // Elements in `tmp_rowset_keys` has the same lifetime as `it`
    std::vector<std::string_view> stage_keys;
    auto recycle_func = [&start_time, &num_scanned, &num_recycled, &stage_keys, this](
                                std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleStagePB recycle_stage;
        if (!recycle_stage.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle stage").tag("key", hex(k));
            return -1;
        }

        int idx = stoi(recycle_stage.stage().obj_info().id());
        if (idx > instance_info_.obj_info().size() || idx < 1) {
            LOG(WARNING) << "invalid idx: " << idx;
            return -1;
        }

        std::shared_ptr<StorageVaultAccessor> accessor;
        int ret = SYNC_POINT_HOOK_RETURN_VALUE(
                [&] {
                    auto& old_obj = instance_info_.obj_info()[idx - 1];
                    auto s3_conf = S3Conf::from_obj_store_info(old_obj);
                    if (!s3_conf) {
                        return -1;
                    }

                    s3_conf->prefix = recycle_stage.stage().obj_info().prefix();
                    std::shared_ptr<S3Accessor> s3_accessor;
                    int ret = S3Accessor::create(std::move(s3_conf.value()), &s3_accessor);
                    if (ret != 0) {
                        return -1;
                    }

                    accessor = std::move(s3_accessor);
                    return 0;
                }(),
                "recycle_stage:get_accessor", &accessor);

        if (ret != 0) {
            LOG(WARNING) << "failed to init accessor ret=" << ret;
            return ret;
        }

        LOG_INFO("begin to delete objects of dropped internal stage")
                .tag("instance_id", instance_id_)
                .tag("stage_id", recycle_stage.stage().stage_id())
                .tag("user_name", recycle_stage.stage().mysql_user_name()[0])
                .tag("user_id", recycle_stage.stage().mysql_user_id()[0])
                .tag("obj_info_id", idx)
                .tag("prefix", recycle_stage.stage().obj_info().prefix());
        ret = accessor->delete_all();
        if (ret != 0) {
            LOG(WARNING) << "failed to delete objects of dropped internal stage. instance_id="
                         << instance_id_ << ", stage_id=" << recycle_stage.stage().stage_id()
                         << ", prefix=" << recycle_stage.stage().obj_info().prefix()
                         << ", ret=" << ret;
            return -1;
        }
        ++num_recycled;
        check_recycle_task(instance_id_, "recycle_stage", num_scanned, num_recycled, start_time);
        stage_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&stage_keys, this]() -> int {
        if (stage_keys.empty()) return 0;
        std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                              [&](int*) { stage_keys.clear(); });
        if (0 != txn_remove(txn_kv_.get(), stage_keys)) {
            LOG(WARNING) << "failed to delete recycle partition kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };

    return scan_and_recycle(key0, key1, std::move(recycle_func), std::move(loop_done));
}

int InstanceRecycler::recycle_expired_stage_objects() {
    LOG_INFO("begin to recycle expired stage objects").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();

    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        LOG_INFO("recycle expired stage objects, cost={}s", cost).tag("instance_id", instance_id_);
    });
    int ret = 0;
    for (const auto& stage : instance_info_.stages()) {
        if (stopped()) break;
        if (stage.type() == StagePB::EXTERNAL) {
            continue;
        }
        int idx = stoi(stage.obj_info().id());
        if (idx > instance_info_.obj_info().size() || idx < 1) {
            LOG(WARNING) << "invalid idx: " << idx << ", id: " << stage.obj_info().id();
            continue;
        }

        const auto& old_obj = instance_info_.obj_info()[idx - 1];
        auto s3_conf = S3Conf::from_obj_store_info(old_obj);
        if (!s3_conf) {
            LOG(WARNING) << "failed to init accessor";
            continue;
        }

        s3_conf->prefix = stage.obj_info().prefix();
        std::shared_ptr<S3Accessor> accessor;
        int ret1 = S3Accessor::create(std::move(*s3_conf), &accessor);
        if (ret1 != 0) {
            LOG(WARNING) << "failed to init s3 accessor ret=" << ret1;
            ret = -1;
            continue;
        }

        LOG(INFO) << "recycle expired stage objects, instance_id=" << instance_id_
                  << ", stage_id=" << stage.stage_id()
                  << ", user_name=" << stage.mysql_user_name().at(0)
                  << ", user_id=" << stage.mysql_user_id().at(0)
                  << ", prefix=" << stage.obj_info().prefix();
        int64_t expiration_time =
                duration_cast<seconds>(system_clock::now().time_since_epoch()).count() -
                config::internal_stage_objects_expire_time_second;
        ret1 = accessor->delete_all(expiration_time);
        if (ret1 != 0) {
            LOG(WARNING) << "failed to recycle expired stage objects, instance_id=" << instance_id_
                         << ", stage_id=" << stage.stage_id() << ", ret=" << ret1;
            ret = -1;
            continue;
        }
    }
    return ret;
}

void InstanceRecycler::register_recycle_task(const std::string& task_name, int64_t start_time) {
    std::lock_guard lock(recycle_tasks_mutex);
    running_recycle_tasks[task_name] = start_time;
}

void InstanceRecycler::unregister_recycle_task(const std::string& task_name) {
    std::lock_guard lock(recycle_tasks_mutex);
    DCHECK(running_recycle_tasks[task_name] > 0);
    running_recycle_tasks.erase(task_name);
}

bool InstanceRecycler::check_recycle_tasks() {
    std::map<std::string, int64_t> tmp_running_recycle_tasks;
    {
        std::lock_guard lock(recycle_tasks_mutex);
        tmp_running_recycle_tasks = running_recycle_tasks;
    }

    bool found = false;
    int64_t now = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    for (auto& [task_name, start_time] : tmp_running_recycle_tasks) {
        int64_t cost = now - start_time;
        if (cost > config::recycle_task_threshold_seconds) [[unlikely]] {
            LOG_INFO("recycle task cost too much time cost={}s", cost)
                    .tag("instance_id", instance_id_)
                    .tag("task", task_name);
            found = true;
        }
    }

    return found;
}

} // namespace doris::cloud
