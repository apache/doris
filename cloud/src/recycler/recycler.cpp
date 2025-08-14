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

#include <brpc/builtin_service.pb.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <initializer_list>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>

#include "common/defer.h"
#include "common/stopwatch.h"
#include "meta-service/meta_service.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/meta_service_schema.h"
#include "meta-store/blob_message.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/checker.h"
#include "recycler/hdfs_accessor.h"
#include "recycler/s3_accessor.h"
#include "recycler/storage_vault_accessor.h"
#ifdef UNIT_TEST
#include "../test/mock_accessor.h"
#endif
#include "common/bvars.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-store/keys.h"
#include "recycler/recycler_service.h"
#include "recycler/sync_executor.h"
#include "recycler/util.h"

namespace doris::cloud {

using namespace std::chrono;

RecyclerMetricsContext tablet_metrics_context_("global_recycler", "recycle_tablet");
RecyclerMetricsContext segment_metrics_context_("global_recycler", "recycle_segment");

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

void scan_restore_job_rowset(
        Transaction* txn, const std::string& instance_id, int64_t tablet_id, MetaServiceCode& code,
        std::string& msg,
        std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>>* restore_job_rs_metas);

static inline void check_recycle_task(const std::string& instance_id, const std::string& task_name,
                                      int64_t num_scanned, int64_t num_recycled,
                                      int64_t start_time) {
    if ((num_scanned % 10000) == 0 && (num_scanned > 0)) [[unlikely]] {
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        if (cost > config::recycle_task_threshold_seconds) {
            LOG_WARNING("recycle task cost too much time cost={}s", cost)
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

    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism,
                                                               "s3_producer_pool");
    s3_producer_pool->start();
    auto recycle_tablet_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism,
                                                                  "recycle_tablet_pool");
    recycle_tablet_pool->start();
    auto group_recycle_function_pool = std::make_shared<SimpleThreadPool>(
            config::recycle_pool_parallelism, "group_recycle_function_pool");
    group_recycle_function_pool->start();
    _thread_pool_group =
            RecyclerThreadPoolGroup(std::move(s3_producer_pool), std::move(recycle_tablet_pool),
                                    std::move(group_recycle_function_pool));

    txn_lazy_committer_ = std::make_shared<TxnLazyCommitter>(txn_kv_);
}

Recycler::~Recycler() {
    if (!stopped()) {
        stop();
    }
}

void Recycler::instance_scanner_callback() {
    // sleep 60 seconds before scheduling for the launch procedure to complete:
    // some bad hdfs connection may cause some log to stdout stderr
    // which may pollute .out file and affect the script to check success
    std::this_thread::sleep_for(
            std::chrono::seconds(config::recycler_sleep_before_scheduling_seconds));
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
        auto instance_recycler = std::make_shared<InstanceRecycler>(
                txn_kv_, instance, _thread_pool_group, txn_lazy_committer_);

        if (int r = instance_recycler->init(); r != 0) {
            LOG(WARNING) << "failed to init instance recycler, instance_id=" << instance_id
                         << " ret=" << r;
            continue;
        }
        std::string recycle_job_key;
        job_recycle_key({instance_id}, &recycle_job_key);
        int ret = prepare_instance_recycle_job(txn_kv_.get(), recycle_job_key, instance_id,
                                               ip_port_, config::recycle_interval_seconds * 1000);
        if (ret != 0) { // Prepare failed
            LOG(WARNING) << "failed to prepare recycle_job, instance_id=" << instance_id
                         << " ret=" << ret;
            continue;
        } else {
            std::lock_guard lock(mtx_);
            recycling_instance_map_.emplace(instance_id, instance_recycler);
        }
        if (stopped()) return;
        LOG_WARNING("begin to recycle instance").tag("instance_id", instance_id);
        auto ctime_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        g_bvar_recycler_instance_recycle_task_concurrency << 1;
        g_bvar_recycler_instance_running_counter << 1;
        g_bvar_recycler_instance_recycle_start_ts.put({instance_id}, ctime_ms);
        tablet_metrics_context_.reset();
        segment_metrics_context_.reset();
        ret = instance_recycler->do_recycle();
        tablet_metrics_context_.finish_report();
        segment_metrics_context_.finish_report();
        g_bvar_recycler_instance_recycle_task_concurrency << -1;
        g_bvar_recycler_instance_running_counter << -1;
        // If instance recycler has been aborted, don't finish this job
        if (!instance_recycler->stopped()) {
            finish_instance_recycle_job(txn_kv_.get(), recycle_job_key, instance_id, ip_port_,
                                        ret == 0, ctime_ms);
        }
        {
            std::lock_guard lock(mtx_);
            recycling_instance_map_.erase(instance_id);
        }

        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        auto elpased_ms = now - ctime_ms;
        g_bvar_recycler_instance_recycle_end_ts.put({instance_id}, now);
        g_bvar_recycler_instance_last_round_recycle_duration.put({instance_id}, elpased_ms);
        g_bvar_recycler_instance_next_ts.put({instance_id},
                                             now + config::recycle_interval_seconds * 1000);
        LOG(INFO) << "recycle instance done, "
                  << "instance_id=" << instance_id << " ret=" << ret << " ctime_ms: " << ctime_ms
                  << " now: " << now;

        g_bvar_recycler_instance_recycle_last_success_ts.put({instance_id}, now);

        LOG_WARNING("finish recycle instance")
                .tag("instance_id", instance_id)
                .tag("cost_ms", elpased_ms);
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
    g_bvar_recycler_task_max_concurrency.set_value(config::recycle_concurrency);
    S3Environment::getInstance();

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
        auto recycler_service =
                new RecyclerServiceImpl(txn_kv_, this, checker_.get(), txn_lazy_committer_);
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
    int get(int64_t index_id, int32_t schema_version, InvertedIndexInfo& res) {
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
        err = cloud::blob_get(txn.get(), schema_key, &val_buf);
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
            InvertedIndexStorageFormatPB index_format = InvertedIndexStorageFormatPB::V1;
            if (schema.has_inverted_index_storage_format()) {
                index_format = schema.inverted_index_storage_format();
            }
            res.first = index_format;
            res.second.reserve(schema.index_size());
            for (auto& i : schema.index()) {
                if (i.has_index_type() && i.index_type() == IndexType::INVERTED) {
                    res.second.push_back(std::make_pair(i.index_id(), i.index_suffix_name()));
                }
            }
        }
        insert(index_id, schema_version, res);
        return 0;
    }

    // Empty `ids` means this schema has no inverted index
    void insert(int64_t index_id, int32_t schema_version, const InvertedIndexInfo& index_info) {
        if (index_info.second.empty()) {
            TEST_SYNC_POINT("InvertedIndexIdCache::insert1");
            std::lock_guard lock(mtx_);
            schemas_without_inverted_index_.emplace(index_id, schema_version);
        } else {
            TEST_SYNC_POINT("InvertedIndexIdCache::insert2");
            std::lock_guard lock(mtx_);
            inverted_index_id_map_.try_emplace({index_id, schema_version}, index_info);
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
    std::unordered_map<Key, InvertedIndexInfo, HashOfKey> inverted_index_id_map_;
    // Store <index_id, schema_version> of schema which doesn't have inverted index
    std::unordered_set<Key, HashOfKey> schemas_without_inverted_index_;
};

InstanceRecycler::InstanceRecycler(std::shared_ptr<TxnKv> txn_kv, const InstanceInfoPB& instance,
                                   RecyclerThreadPoolGroup thread_pool_group,
                                   std::shared_ptr<TxnLazyCommitter> txn_lazy_committer)
        : txn_kv_(std::move(txn_kv)),
          instance_id_(instance.instance_id()),
          instance_info_(instance),
          inverted_index_id_cache_(std::make_unique<InvertedIndexIdCache>(instance_id_, txn_kv_)),
          _thread_pool_group(std::move(thread_pool_group)),
          txn_lazy_committer_(std::move(txn_lazy_committer)) {};

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

    FullRangeGetOptions opts(txn_kv_);
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
        std::string recycler_storage_vault_white_list = accumulate(
                config::recycler_storage_vault_white_list.begin(),
                config::recycler_storage_vault_white_list.end(), std::string(),
                [](std::string a, std::string b) { return a + (a.empty() ? "" : ",") + b; });
        LOG_INFO("config::recycler_storage_vault_white_list")
                .tag("", recycler_storage_vault_white_list);
        if (!config::recycler_storage_vault_white_list.empty()) {
            if (auto it = std::find(config::recycler_storage_vault_white_list.begin(),
                                    config::recycler_storage_vault_white_list.end(), vault.name());
                it == config::recycler_storage_vault_white_list.end()) {
                LOG_WARNING(
                        "failed to init accessor for vault because this vault is not in "
                        "config::recycler_storage_vault_white_list. ")
                        .tag(" vault name:", vault.name())
                        .tag(" config::recycler_storage_vault_white_list:",
                             recycler_storage_vault_white_list);
                continue;
            }
        }
        TEST_SYNC_POINT_CALLBACK("InstanceRecycler::init_storage_vault_accessors.mock_vault",
                                 &accessor_map_, &vault);
        if (vault.has_hdfs_info()) {
            auto accessor = std::make_shared<HdfsAccessor>(vault.hdfs_info());
            int ret = accessor->init();
            if (ret != 0) {
                LOG(WARNING) << "failed to init hdfs accessor. instance_id=" << instance_id_
                             << " resource_id=" << vault.id() << " name=" << vault.name()
                             << " hdfs_vault=" << vault.hdfs_info().ShortDebugString();
                continue;
            }
            LOG(INFO) << "succeed to init hdfs accessor. instance_id=" << instance_id_
                      << " resource_id=" << vault.id() << " name=" << vault.name()
                      << " hdfs_vault=" << vault.hdfs_info().ShortDebugString();
            accessor_map_.emplace(vault.id(), std::move(accessor));
        } else if (vault.has_obj_info()) {
            auto s3_conf = S3Conf::from_obj_store_info(vault.obj_info());
            if (!s3_conf) {
                LOG(WARNING) << "failed to init object accessor, invalid conf, instance_id="
                             << instance_id_ << " s3_vault=" << vault.obj_info().ShortDebugString();
                continue;
            }

            std::shared_ptr<S3Accessor> accessor;
            int ret = S3Accessor::create(*s3_conf, &accessor);
            if (ret != 0) {
                LOG(WARNING) << "failed to init s3 accessor. instance_id=" << instance_id_
                             << " resource_id=" << vault.id() << " name=" << vault.name()
                             << " ret=" << ret
                             << " s3_vault=" << encryt_sk(vault.obj_info().ShortDebugString());
                continue;
            }
            LOG(INFO) << "succeed to init s3 accessor. instance_id=" << instance_id_
                      << " resource_id=" << vault.id() << " name=" << vault.name() << " ret=" << ret
                      << " s3_vault=" << encryt_sk(vault.obj_info().ShortDebugString());
            accessor_map_.emplace(vault.id(), std::move(accessor));
        }
    }

    if (!it->is_valid()) {
        LOG_WARNING("failed to get storage vault kv");
        return -1;
    }

    if (accessor_map_.empty()) {
        LOG(WARNING) << "no accessors for instance=" << instance_id_;
        return -2;
    }
    LOG_INFO("finish init instance recycler number_accessors={} instance=", accessor_map_.size(),
             instance_id_);

    return 0;
}

int InstanceRecycler::init() {
    int ret = init_obj_store_accessors();
    if (ret != 0) {
        return ret;
    }

    return init_storage_vault_accessors();
}

template <typename... Func>
auto task_wrapper(Func... funcs) -> std::function<int()> {
    return [funcs...]() {
        return [](std::initializer_list<int> ret_vals) {
            int i = 0;
            for (int ret : ret_vals) {
                if (ret != 0) {
                    i = ret;
                }
            }
            return i;
        }({funcs()...});
    };
}

int InstanceRecycler::do_recycle() {
    TEST_SYNC_POINT("InstanceRecycler.do_recycle");
    if (instance_info_.status() == InstanceInfoPB::DELETED) {
        return recycle_deleted_instance();
    } else if (instance_info_.status() == InstanceInfoPB::NORMAL) {
        SyncExecutor<int> sync_executor(_thread_pool_group.group_recycle_function_pool,
                                        fmt::format("instance id {}", instance_id_),
                                        [](int r) { return r != 0; });
        sync_executor
                .add(task_wrapper( // dropped table and dropped partition need to be recycled in series
                                   // becase they may both recycle the same set of tablets
                        // recycle dropped table or idexes(mv, rollup)
                        [this]() -> int { return InstanceRecycler::recycle_indexes(); },
                        // recycle dropped partitions
                        [this]() -> int { return InstanceRecycler::recycle_partitions(); }))
                .add(task_wrapper(
                        [this]() -> int { return InstanceRecycler::recycle_tmp_rowsets(); }))
                .add(task_wrapper([this]() -> int { return InstanceRecycler::recycle_rowsets(); }))
                .add(task_wrapper(
                        [this]() { return InstanceRecycler::abort_timeout_txn(); },
                        [this]() { return InstanceRecycler::recycle_expired_txn_label(); }))
                .add(task_wrapper([this]() { return InstanceRecycler::recycle_copy_jobs(); }))
                .add(task_wrapper([this]() { return InstanceRecycler::recycle_stage(); }))
                .add(task_wrapper(
                        [this]() { return InstanceRecycler::recycle_expired_stage_objects(); }))
                .add(task_wrapper([this]() { return InstanceRecycler::recycle_versions(); }))
                .add(task_wrapper([this]() { return InstanceRecycler::recycle_operation_logs(); }))
                .add(task_wrapper([this]() { return InstanceRecycler::recycle_restore_jobs(); }));
        bool finished = true;
        std::vector<int> rets = sync_executor.when_all(&finished);
        for (int ret : rets) {
            if (ret != 0) {
                return ret;
            }
        }
        return finished ? 0 : -1;
    } else {
        LOG(WARNING) << "invalid instance status: " << instance_info_.status()
                     << " instance_id=" << instance_id_;
        return -1;
    }
}

/**
* 1. delete all remote data
* 2. delete all kv
* 3. remove instance kv
*/
int InstanceRecycler::recycle_deleted_instance() {
    LOG_WARNING("begin to recycle deleted instance").tag("instance_id", instance_id_);

    int ret = 0;
    auto start_time = steady_clock::now();

    DORIS_CLOUD_DEFER {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(WARNING) << (ret == 0 ? "successfully" : "failed to")
                     << " recycle deleted instance, cost=" << cost
                     << "s, instance_id=" << instance_id_;
    };

    // delete all remote data
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

    if (ret != 0) {
        LOG(WARNING) << "failed to delete all data of deleted instance=" << instance_id_;
        return ret;
    }

    // delete all kv
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

bool is_txn_finished(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id,
                     int64_t txn_id) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn, txn_id=" << txn_id << " instance_id=" << instance_id;
        return false;
    }

    std::string index_val;
    const std::string index_key = txn_index_key({instance_id, txn_id});
    err = txn->get(index_key, &index_val);
    if (err != TxnErrorCode::TXN_OK) {
        if (TxnErrorCode::TXN_KEY_NOT_FOUND == err) {
            TEST_SYNC_POINT_CALLBACK("is_txn_finished::txn_has_been_recycled");
            // txn has been recycled;
            LOG(INFO) << "txn index key has been recycled, txn_id=" << txn_id
                      << " instance_id=" << instance_id;
            return true;
        }
        LOG(WARNING) << "failed to get txn index key, txn_id=" << txn_id
                     << " instance_id=" << instance_id << " key=" << hex(index_key)
                     << " err=" << err;
        return false;
    }

    TxnIndexPB index_pb;
    if (!index_pb.ParseFromString(index_val)) {
        LOG(WARNING) << "failed to parse txn_index_pb, txn_id=" << txn_id
                     << " instance_id=" << instance_id;
        return false;
    }

    DCHECK(index_pb.has_tablet_index() == true);
    if (!index_pb.tablet_index().has_db_id()) {
        // In the previous version, the db_id was not set in the index_pb.
        // If updating to the version which enable txn lazy commit, the db_id will be set.
        LOG(INFO) << "txn index has no db_id, txn_id=" << txn_id << " instance_id=" << instance_id
                  << " index=" << index_pb.ShortDebugString();
        return true;
    }

    int64_t db_id = index_pb.tablet_index().db_id();
    DCHECK_GT(db_id, 0) << "db_id=" << db_id << " txn_id=" << txn_id
                        << " instance_id=" << instance_id;

    std::string info_val;
    const std::string info_key = txn_info_key({instance_id, db_id, txn_id});
    err = txn->get(info_key, &info_val);
    if (err != TxnErrorCode::TXN_OK) {
        if (TxnErrorCode::TXN_KEY_NOT_FOUND == err) {
            // txn info has been recycled;
            LOG(INFO) << "txn info key has been recycled, db_id=" << db_id << " txn_id=" << txn_id
                      << " instance_id=" << instance_id;
            return true;
        }

        DCHECK(err != TxnErrorCode::TXN_KEY_NOT_FOUND);
        LOG(WARNING) << "failed to get txn info key, txn_id=" << txn_id
                     << " instance_id=" << instance_id << " key=" << hex(info_key)
                     << " err=" << err;
        return false;
    }

    TxnInfoPB txn_info;
    if (!txn_info.ParseFromString(info_val)) {
        LOG(WARNING) << "failed to parse txn_info, txn_id=" << txn_id
                     << " instance_id=" << instance_id;
        return false;
    }

    DCHECK(txn_info.txn_id() == txn_id) << "txn_id=" << txn_id << " instance_id=" << instance_id
                                        << " txn_info=" << txn_info.ShortDebugString();

    if (TxnStatusPB::TXN_STATUS_ABORTED == txn_info.status() ||
        TxnStatusPB::TXN_STATUS_VISIBLE == txn_info.status()) {
        TEST_SYNC_POINT_CALLBACK("is_txn_finished::txn_has_been_aborted", &txn_info);
        return true;
    }

    TEST_SYNC_POINT_CALLBACK("is_txn_finished::txn_not_finished", &txn_info);
    return false;
}

int64_t calculate_rowset_expired_time(const std::string& instance_id_, const RecycleRowsetPB& rs,
                                      int64_t* earlest_ts /* rowset earliest expiration ts */) {
    if (config::force_immediate_recycle) {
        return 0L;
    }
    // RecycleRowsetPB created by compacted or dropped rowset has no expiration time, and will be recycled when exceed retention time
    int64_t expiration = rs.expiration() > 0 ? rs.expiration() : rs.creation_time();
    int64_t retention_seconds = config::retention_seconds;
    if (rs.type() == RecycleRowsetPB::COMPACT || rs.type() == RecycleRowsetPB::DROP) {
        retention_seconds = std::min(config::compacted_rowset_retention_seconds, retention_seconds);
    }
    int64_t final_expiration = expiration + retention_seconds;
    if (*earlest_ts > final_expiration) {
        *earlest_ts = final_expiration;
        g_bvar_recycler_recycle_rowset_earlest_ts.put(instance_id_, *earlest_ts);
    }
    return final_expiration;
}

int64_t calculate_partition_expired_time(
        const std::string& instance_id_, const RecyclePartitionPB& partition_meta_pb,
        int64_t* earlest_ts /* partition earliest expiration ts */) {
    if (config::force_immediate_recycle) {
        return 0L;
    }
    int64_t expiration = partition_meta_pb.expiration() > 0 ? partition_meta_pb.expiration()
                                                            : partition_meta_pb.creation_time();
    int64_t retention_seconds = config::retention_seconds;
    if (partition_meta_pb.state() == RecyclePartitionPB::DROPPED) {
        retention_seconds =
                std::min(config::dropped_partition_retention_seconds, retention_seconds);
    }
    int64_t final_expiration = expiration + retention_seconds;
    if (*earlest_ts > final_expiration) {
        *earlest_ts = final_expiration;
        g_bvar_recycler_recycle_partition_earlest_ts.put(instance_id_, *earlest_ts);
    }
    return final_expiration;
}

int64_t calculate_index_expired_time(const std::string& instance_id_,
                                     const RecycleIndexPB& index_meta_pb,
                                     int64_t* earlest_ts /* index earliest expiration ts */) {
    if (config::force_immediate_recycle) {
        return 0L;
    }
    int64_t expiration = index_meta_pb.expiration() > 0 ? index_meta_pb.expiration()
                                                        : index_meta_pb.creation_time();
    int64_t retention_seconds = config::retention_seconds;
    if (index_meta_pb.state() == RecycleIndexPB::DROPPED) {
        retention_seconds = std::min(config::dropped_index_retention_seconds, retention_seconds);
    }
    int64_t final_expiration = expiration + retention_seconds;
    if (*earlest_ts > final_expiration) {
        *earlest_ts = final_expiration;
        g_bvar_recycler_recycle_index_earlest_ts.put(instance_id_, *earlest_ts);
    }
    return final_expiration;
}

int64_t calculate_tmp_rowset_expired_time(
        const std::string& instance_id_, const doris::RowsetMetaCloudPB& tmp_rowset_meta_pb,
        int64_t* earlest_ts /* tmp_rowset earliest expiration ts */) {
    // ATTN: `txn_expiration` should > 0, however we use `creation_time` + a large `retention_time` (> 1 day in production environment)
    //  when `txn_expiration` <= 0 in some unexpected situation (usually when there are bugs). This is usually safe, coz loading
    //  duration or timeout always < `retention_time` in practice.
    int64_t expiration = tmp_rowset_meta_pb.txn_expiration() > 0
                                 ? tmp_rowset_meta_pb.txn_expiration()
                                 : tmp_rowset_meta_pb.creation_time();
    expiration = config::force_immediate_recycle ? 0 : expiration;
    int64_t final_expiration = expiration + config::retention_seconds;
    if (*earlest_ts > final_expiration) {
        *earlest_ts = final_expiration;
        g_bvar_recycler_recycle_tmp_rowset_earlest_ts.put(instance_id_, *earlest_ts);
    }
    return final_expiration;
}

int64_t calculate_txn_expired_time(const std::string& instance_id_, const RecycleTxnPB& txn_meta_pb,
                                   int64_t* earlest_ts /* txn earliest expiration ts */) {
    int64_t final_expiration = txn_meta_pb.creation_time() + config::label_keep_max_second * 1000L;
    if (*earlest_ts > final_expiration / 1000) {
        *earlest_ts = final_expiration / 1000;
        g_bvar_recycler_recycle_expired_txn_label_earlest_ts.put(instance_id_, *earlest_ts);
    }
    return final_expiration;
}

int64_t calculate_restore_job_expired_time(
        const std::string& instance_id_, const RestoreJobCloudPB& restore_job,
        int64_t* earlest_ts /* restore job earliest expiration ts */) {
    if (config::force_immediate_recycle || restore_job.state() == RestoreJobCloudPB::DROPPED ||
        restore_job.state() == RestoreJobCloudPB::COMPLETED) {
        // final state, recycle immediately
        return 0L;
    }
    // not final state, wait much longer than the FE's timeout(1 day)
    int64_t last_modified_s =
            restore_job.has_mtime_s() ? restore_job.mtime_s() : restore_job.ctime_s();
    int64_t expiration = restore_job.expired_at_s() > 0
                                 ? last_modified_s + restore_job.expired_at_s()
                                 : last_modified_s;
    int64_t final_expiration = expiration + config::retention_seconds;
    if (*earlest_ts > final_expiration) {
        *earlest_ts = final_expiration;
        g_bvar_recycler_recycle_restore_job_earlest_ts.put(instance_id_, *earlest_ts);
    }
    return final_expiration;
}

int InstanceRecycler::recycle_indexes() {
    const std::string task_name = "recycle_indexes";
    int64_t num_scanned = 0;
    int64_t num_expired = 0;
    int64_t num_recycled = 0;
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    RecycleIndexKeyInfo index_key_info0 {instance_id_, 0};
    RecycleIndexKeyInfo index_key_info1 {instance_id_, INT64_MAX};
    std::string index_key0;
    std::string index_key1;
    recycle_index_key(index_key_info0, &index_key0);
    recycle_index_key(index_key_info1, &index_key1);

    LOG_WARNING("begin to recycle indexes").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle indexes finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    };

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

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
        if (current_time <
            calculate_index_expired_time(instance_id_, index_pb, &earlest_ts)) { // not expired
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
        if (recycle_tablets(index_pb.table_id(), index_id, metrics_context) != 0) {
            LOG_WARNING("failed to recycle tablets under index")
                    .tag("table_id", index_pb.table_id())
                    .tag("instance_id", instance_id_)
                    .tag("index_id", index_id);
            return -1;
        }

        if (index_pb.has_db_id()) {
            // Recycle the versioned keys
            std::unique_ptr<Transaction> txn;
            err = txn_kv_->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to create txn").tag("err", err);
                return -1;
            }
            std::string meta_key = versioned::meta_index_key({instance_id_, index_id});
            std::string index_key = versioned::index_index_key({instance_id_, index_id});
            std::string index_inverted_key = versioned::index_inverted_key(
                    {instance_id_, index_pb.db_id(), index_pb.table_id(), index_id});
            versioned_remove_all(txn.get(), meta_key);
            txn->remove(index_key);
            txn->remove(index_inverted_key);
            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to commit txn").tag("err", err);
                return -1;
            }
        }

        metrics_context.total_recycled_num = ++num_recycled;
        metrics_context.report();
        check_recycle_task(instance_id_, task_name, num_scanned, num_recycled, start_time);
        index_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&index_keys, this]() -> int {
        if (index_keys.empty()) return 0;
        DORIS_CLOUD_DEFER {
            index_keys.clear();
        };
        if (0 != txn_remove(txn_kv_.get(), index_keys)) {
            LOG(WARNING) << "failed to delete recycle index kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_indexes();
    }
    // recycle_func and loop_done for scan and recycle
    return scan_and_recycle(index_key0, index_key1, std::move(recycle_func), std::move(loop_done));
}

bool check_lazy_txn_finished(std::shared_ptr<TxnKv> txn_kv, const std::string instance_id,
                             int64_t tablet_id) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("check_lazy_txn_finished::bypass_check", true);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn, instance_id=" << instance_id
                     << " tablet_id=" << tablet_id << " err=" << err;
        return false;
    }

    std::string tablet_idx_key = meta_tablet_idx_key({instance_id, tablet_id});
    std::string tablet_idx_val;
    err = txn->get(tablet_idx_key, &tablet_idx_val);
    if (TxnErrorCode::TXN_OK != err) {
        LOG(WARNING) << "failed to get tablet index, instance_id=" << instance_id
                     << " tablet_id=" << tablet_id << " err=" << err
                     << " key=" << hex(tablet_idx_key);
        return false;
    }

    TabletIndexPB tablet_idx_pb;
    if (!tablet_idx_pb.ParseFromString(tablet_idx_val)) {
        LOG(WARNING) << "failed to parse tablet_idx_pb, instance_id=" << instance_id
                     << " tablet_id=" << tablet_id;
        return false;
    }

    if (!tablet_idx_pb.has_db_id()) {
        // In the previous version, the db_id was not set in the index_pb.
        // If updating to the version which enable txn lazy commit, the db_id will be set.
        LOG(INFO) << "txn index has no db_id, tablet_id=" << tablet_id
                  << " instance_id=" << instance_id
                  << " tablet_idx_pb=" << tablet_idx_pb.ShortDebugString();
        return true;
    }

    std::string ver_val;
    std::string ver_key =
            partition_version_key({instance_id, tablet_idx_pb.db_id(), tablet_idx_pb.table_id(),
                                   tablet_idx_pb.partition_id()});
    err = txn->get(ver_key, &ver_val);

    if (TxnErrorCode::TXN_KEY_NOT_FOUND == err) {
        LOG(INFO) << ""
                     "partition version not found, instance_id="
                  << instance_id << " db_id=" << tablet_idx_pb.db_id()
                  << " table_id=" << tablet_idx_pb.table_id()
                  << " partition_id=" << tablet_idx_pb.partition_id() << " tablet_id=" << tablet_id
                  << " key=" << hex(ver_key);
        return true;
    }

    if (TxnErrorCode::TXN_OK != err) {
        LOG(WARNING) << "failed to get partition version, instance_id=" << instance_id
                     << " db_id=" << tablet_idx_pb.db_id()
                     << " table_id=" << tablet_idx_pb.table_id()
                     << " partition_id=" << tablet_idx_pb.partition_id()
                     << " tablet_id=" << tablet_id << " key=" << hex(ver_key) << " err=" << err;
        return false;
    }

    VersionPB version_pb;
    if (!version_pb.ParseFromString(ver_val)) {
        LOG(WARNING) << "failed to parse version_pb, instance_id=" << instance_id
                     << " db_id=" << tablet_idx_pb.db_id()
                     << " table_id=" << tablet_idx_pb.table_id()
                     << " partition_id=" << tablet_idx_pb.partition_id()
                     << " tablet_id=" << tablet_id << " key=" << hex(ver_key);
        return false;
    }

    if (version_pb.pending_txn_ids_size() > 0) {
        TEST_SYNC_POINT_CALLBACK("check_lazy_txn_finished::txn_not_finished");
        DCHECK(version_pb.pending_txn_ids_size() == 1);
        LOG(WARNING) << "lazy txn not finished, instance_id=" << instance_id
                     << " db_id=" << tablet_idx_pb.db_id()
                     << " table_id=" << tablet_idx_pb.table_id()
                     << " partition_id=" << tablet_idx_pb.partition_id()
                     << " tablet_id=" << tablet_id << " txn_id=" << version_pb.pending_txn_ids(0)
                     << " key=" << hex(ver_key);
        return false;
    }
    return true;
}

int InstanceRecycler::recycle_partitions() {
    const std::string task_name = "recycle_partitions";
    int64_t num_scanned = 0;
    int64_t num_expired = 0;
    int64_t num_recycled = 0;
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    RecyclePartKeyInfo part_key_info0 {instance_id_, 0};
    RecyclePartKeyInfo part_key_info1 {instance_id_, INT64_MAX};
    std::string part_key0;
    std::string part_key1;
    recycle_partition_key(part_key_info0, &part_key0);
    recycle_partition_key(part_key_info1, &part_key1);

    LOG_WARNING("begin to recycle partitions").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle partitions finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    };

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

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
        if (current_time <
            calculate_partition_expired_time(instance_id_, part_pb, &earlest_ts)) { // not expired
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
            if (recycle_tablets(part_pb.table_id(), index_id, metrics_context, partition_id) != 0) {
                LOG_WARNING("failed to recycle tablets under partition")
                        .tag("table_id", part_pb.table_id())
                        .tag("instance_id", instance_id_)
                        .tag("index_id", index_id)
                        .tag("partition_id", partition_id);
                ret = -1;
            }
        }
        if (ret == 0 && part_pb.has_db_id()) {
            // Recycle the versioned keys
            std::unique_ptr<Transaction> txn;
            err = txn_kv_->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to create txn").tag("err", err);
                return -1;
            }
            std::string meta_key = versioned::meta_partition_key({instance_id_, partition_id});
            std::string index_key = versioned::partition_index_key({instance_id_, partition_id});
            std::string inverted_index_key = versioned::partition_inverted_index_key(
                    {instance_id_, part_pb.db_id(), part_pb.table_id(), partition_id});
            versioned_remove_all(txn.get(), meta_key);
            txn->remove(index_key);
            txn->remove(inverted_index_key);
            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to commit txn").tag("err", err);
                return -1;
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
            metrics_context.total_recycled_num = num_recycled;
            metrics_context.report();
        }
        return ret;
    };

    auto loop_done = [&partition_keys, &partition_version_keys, this]() -> int {
        if (partition_keys.empty()) return 0;
        DORIS_CLOUD_DEFER {
            partition_keys.clear();
            partition_version_keys.clear();
        };
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

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_partitions();
    }
    // recycle_func and loop_done for scan and recycle
    return scan_and_recycle(part_key0, part_key1, std::move(recycle_func), std::move(loop_done));
}

int InstanceRecycler::recycle_versions() {
    int64_t num_scanned = 0;
    int64_t num_recycled = 0;
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_versions");

    LOG_WARNING("begin to recycle table and partition versions").tag("instance_id", instance_id_);

    auto start_time = steady_clock::now();

    DORIS_CLOUD_DEFER {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        metrics_context.finish_report();
        LOG_WARNING("recycle table and partition versions finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    };

    auto version_key_begin = partition_version_key({instance_id_, 0, 0, 0});
    auto version_key_end = partition_version_key({instance_id_, INT64_MAX, 0, 0});
    int64_t last_scanned_table_id = 0;
    bool is_recycled = false; // Is last scanned kv recycled
    auto recycle_func = [&num_scanned, &num_recycled, &last_scanned_table_id, &is_recycled,
                         &metrics_context, this](std::string_view k, std::string_view) {
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
                     << " end=" << hex(partition_version_key_end) << " db_id=" << db_id
                     << " table_id=" << table_id;
        // 2. Remove the table version kv of this table
        auto tbl_version_key = table_version_key({instance_id_, db_id, table_id});
        txn->remove(tbl_version_key);
        LOG(WARNING) << "remove table version kv " << hex(tbl_version_key);
        // 3. Remove mow delete bitmap update lock and tablet job lock
        std::string lock_key = meta_delete_bitmap_update_lock_key({instance_id_, table_id, -1});
        txn->remove(lock_key);
        LOG(WARNING) << "remove delete bitmap update lock kv " << hex(lock_key);
        std::string tablet_job_key_begin = mow_tablet_job_key({instance_id_, table_id, 0});
        std::string tablet_job_key_end = mow_tablet_job_key({instance_id_, table_id, INT64_MAX});
        txn->remove(tablet_job_key_begin, tablet_job_key_end);
        LOG(WARNING) << "remove mow tablet job kv, begin=" << hex(tablet_job_key_begin)
                     << " end=" << hex(tablet_job_key_end) << " db_id=" << db_id
                     << " table_id=" << table_id;
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            return -1;
        }
        metrics_context.total_recycled_num = ++num_recycled;
        metrics_context.report();
        is_recycled = true;
        return 0;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_versions();
    }
    // recycle_func and loop_done for scan and recycle
    return scan_and_recycle(version_key_begin, version_key_end, std::move(recycle_func));
}

int InstanceRecycler::recycle_tablets(int64_t table_id, int64_t index_id,
                                      RecyclerMetricsContext& metrics_context,
                                      int64_t partition_id) {
    bool is_multi_version =
            instance_info_.has_multi_version_status() &&
            instance_info_.multi_version_status() != MultiVersionStatus::MULTI_VERSION_DISABLED;
    int64_t num_scanned = 0;
    std::atomic_long num_recycled = 0;

    std::string tablet_key_begin, tablet_key_end;
    std::string stats_key_begin, stats_key_end;
    std::string job_key_begin, job_key_end;

    std::string tablet_belongs;
    if (partition_id > 0) {
        // recycle tablets in a partition belonging to the index
        meta_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &tablet_key_end);
        stats_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &stats_key_begin);
        stats_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &stats_key_end);
        job_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &job_key_begin);
        job_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &job_key_end);
        tablet_belongs = "partition";
    } else {
        // recycle tablets in the index
        meta_tablet_key({instance_id_, table_id, index_id, 0, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &tablet_key_end);
        stats_tablet_key({instance_id_, table_id, index_id, 0, 0}, &stats_key_begin);
        stats_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &stats_key_end);
        job_tablet_key({instance_id_, table_id, index_id, 0, 0}, &job_key_begin);
        job_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &job_key_end);
        tablet_belongs = "index";
    }

    LOG_INFO("begin to recycle tablets of the " + tablet_belongs)
            .tag("table_id", table_id)
            .tag("index_id", index_id)
            .tag("partition_id", partition_id);

    auto start_time = steady_clock::now();

    DORIS_CLOUD_DEFER {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle tablets of " + tablet_belongs + " finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("table_id", table_id)
                .tag("index_id", index_id)
                .tag("partition_id", partition_id)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    };

    // The first string_view represents the tablet key which has been recycled
    // The second bool represents whether the following fdb's tablet key deletion could be done using range move or not
    using TabletKeyPair = std::pair<std::string_view, bool>;
    SyncExecutor<TabletKeyPair> sync_executor(
            _thread_pool_group.recycle_tablet_pool,
            fmt::format("recycle tablets, tablet id {}, index id {}, partition id {}", table_id,
                        index_id, partition_id),
            [](const TabletKeyPair& k) { return k.first.empty(); });

    // Elements in `tablet_keys` has the same lifetime as `it` in `scan_and_recycle`
    std::vector<std::string> tablet_idx_keys;
    std::vector<std::string> restore_job_keys;
    std::vector<std::string> init_rs_keys;
    std::vector<std::string> tablet_compact_stats_keys;
    auto recycle_func = [&, this](std::string_view k, std::string_view v) -> int {
        bool use_range_remove = true;
        ++num_scanned;
        doris::TabletMetaCloudPB tablet_meta_pb;
        if (!tablet_meta_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed tablet meta").tag("key", hex(k));
            use_range_remove = false;
            return -1;
        }
        int64_t tablet_id = tablet_meta_pb.tablet_id();

        if (!check_lazy_txn_finished(txn_kv_, instance_id_, tablet_meta_pb.tablet_id())) {
            LOG(WARNING) << "lazy txn not finished tablet_meta_pb=" << tablet_meta_pb.tablet_id();
            return -1;
        }

        tablet_idx_keys.push_back(meta_tablet_idx_key({instance_id_, tablet_id}));
        restore_job_keys.push_back(job_restore_tablet_key({instance_id_, tablet_id}));
        if (is_multi_version) {
            tablet_compact_stats_keys.push_back(
                    versioned::tablet_compact_stats_key({instance_id_, tablet_id}));
        }
        TEST_SYNC_POINT_RETURN_WITH_VALUE("recycle_tablet::bypass_check", false);
        sync_executor.add([this, &num_recycled, tid = tablet_id, range_move = use_range_remove,
                           &metrics_context, k]() mutable -> TabletKeyPair {
            if (recycle_tablet(tid, metrics_context) != 0) {
                LOG_WARNING("failed to recycle tablet")
                        .tag("instance_id", instance_id_)
                        .tag("tablet_id", tid);
                range_move = false;
                return {std::string_view(), range_move};
            }
            ++num_recycled;
            LOG(INFO) << "recycle_tablets scan, key=" << (k.empty() ? "(empty)" : hex(k));
            return {k, range_move};
        });
        return 0;
    };

    // TODO(AlexYue): Add one ut to cover use_range_remove = false
    auto loop_done = [&, this]() -> int {
        bool finished = true;
        auto tablet_keys = sync_executor.when_all(&finished);
        if (!finished) {
            LOG_WARNING("failed to recycle tablet").tag("instance_id", instance_id_);
            return -1;
        }
        if (tablet_keys.empty() && tablet_idx_keys.empty()) return 0;
        // sort the vector using key's order
        std::sort(tablet_keys.begin(), tablet_keys.end(),
                  [](const auto& prev, const auto& last) { return prev.first < last.first; });
        bool use_range_remove = true;
        for (auto& [_, remove] : tablet_keys) {
            if (!remove) {
                use_range_remove = remove;
                break;
            }
        }
        DORIS_CLOUD_DEFER {
            tablet_idx_keys.clear();
            restore_job_keys.clear();
            init_rs_keys.clear();
            tablet_compact_stats_keys.clear();
        };
        std::unique_ptr<Transaction> txn;
        if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to delete tablet meta kv, instance_id=" << instance_id_;
            return -1;
        }
        std::string tablet_key_end;
        if (!tablet_keys.empty()) {
            if (use_range_remove) {
                tablet_key_end = std::string(tablet_keys.back().first) + '\x00';
                txn->remove(tablet_keys.front().first, tablet_key_end);
            } else {
                for (auto& [k, _] : tablet_keys) {
                    txn->remove(k);
                }
            }
        }
        if (is_multi_version) {
            for (auto& k : tablet_compact_stats_keys) {
                // Remove all versions of tablet compact stats for recycled tablet
                LOG_INFO("remove versioned tablet compact stats key")
                        .tag("compact_stats_key", hex(k));
                versioned_remove_all(txn.get(), k);
            }
        }
        for (auto& k : tablet_idx_keys) {
            txn->remove(k);
        }
        for (auto& k : restore_job_keys) {
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
    if (ret != 0) {
        LOG(WARNING) << "failed to scan_and_recycle, instance_id=" << instance_id_;
        return ret;
    }

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

int InstanceRecycler::delete_rowset_data(const RowsetMetaCloudPB& rs_meta_pb) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("delete_rowset_data::bypass_check", true);
    int64_t num_segments = rs_meta_pb.num_segments();
    if (num_segments <= 0) return 0;

    // Process inverted indexes
    std::vector<std::pair<int64_t, std::string>> index_ids;
    // default format as v1.
    InvertedIndexStorageFormatPB index_format = InvertedIndexStorageFormatPB::V1;
    bool delete_rowset_data_by_prefix = false;
    if (rs_meta_pb.rowset_state() == RowsetStatePB::BEGIN_PARTIAL_UPDATE) {
        // if rowset state is RowsetStatePB::BEGIN_PARTIAL_UPDATE, the number of segments data
        // may be larger than num_segments field in RowsetMeta, so we need to delete the rowset's data by prefix
        delete_rowset_data_by_prefix = true;
    } else if (rs_meta_pb.has_tablet_schema()) {
        for (const auto& index : rs_meta_pb.tablet_schema().index()) {
            if (index.has_index_type() && index.index_type() == IndexType::INVERTED) {
                index_ids.emplace_back(index.index_id(), index.index_suffix_name());
            }
        }
        if (rs_meta_pb.tablet_schema().has_inverted_index_storage_format()) {
            index_format = rs_meta_pb.tablet_schema().inverted_index_storage_format();
        }
    } else if (!rs_meta_pb.has_index_id() || !rs_meta_pb.has_schema_version()) {
        // schema version and index id are not found, delete rowset data by prefix directly.
        delete_rowset_data_by_prefix = true;
    } else {
        // otherwise, try to get schema kv
        InvertedIndexInfo index_info;
        int inverted_index_get_ret = inverted_index_id_cache_->get(
                rs_meta_pb.index_id(), rs_meta_pb.schema_version(), index_info);
        TEST_SYNC_POINT_CALLBACK("InstanceRecycler::delete_rowset_data.tmp_rowset",
                                 &inverted_index_get_ret);
        if (inverted_index_get_ret == 0) {
            index_format = index_info.first;
            index_ids = index_info.second;
        } else if (inverted_index_get_ret == 1) {
            // 1. Schema kv not found means tablet has been recycled
            // Maybe some tablet recycle failed by some bugs
            // We need to delete again to double check
            // 2. Ensure this operation only deletes tablets and does not perform any operations on indexes,
            // because we are uncertain about the inverted index information.
            // If there are inverted indexes, some data might not be deleted,
            // but this is acceptable as we have made our best effort to delete the data.
            LOG_INFO(
                    "delete rowset data schema kv not found, need to delete again to double "
                    "check")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", rs_meta_pb.tablet_id())
                    .tag("rowset", rs_meta_pb.ShortDebugString());
            // Currently index_ids is guaranteed to be empty,
            // but we clear it again here as a safeguard against future code changes
            // that might cause index_ids to no longer be empty
            index_format = InvertedIndexStorageFormatPB::V2;
            index_ids.clear();
        } else {
            // failed to get schema kv, delete rowset data by prefix directly.
            delete_rowset_data_by_prefix = true;
        }
    }

    if (delete_rowset_data_by_prefix) {
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
    int64_t tablet_id = rs_meta_pb.tablet_id();
    const auto& rowset_id = rs_meta_pb.rowset_id_v2();
    std::vector<std::string> file_paths;
    for (int64_t i = 0; i < num_segments; ++i) {
        file_paths.push_back(segment_path(tablet_id, rowset_id, i));
        if (index_format == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index_id : index_ids) {
                file_paths.push_back(inverted_index_path_v1(tablet_id, rowset_id, i, index_id.first,
                                                            index_id.second));
            }
        } else if (!index_ids.empty()) {
            file_paths.push_back(inverted_index_path_v2(tablet_id, rowset_id, i));
        }
    }

    // TODO(AlexYue): seems could do do batch
    return accessor->delete_files(file_paths);
}

int InstanceRecycler::delete_rowset_data(
        const std::map<std::string, doris::RowsetMetaCloudPB>& rowsets, RowsetRecyclingState type,
        RecyclerMetricsContext& metrics_context) {
    int ret = 0;
    // resource_id -> file_paths
    std::map<std::string, std::vector<std::string>> resource_file_paths;
    // (resource_id, tablet_id, rowset_id)
    std::vector<std::tuple<std::string, int64_t, std::string>> rowsets_delete_by_prefix;
    bool is_formal_rowset = (type == RowsetRecyclingState::FORMAL_ROWSET);

    for (const auto& [_, rs] : rowsets) {
        // we have to treat tmp rowset as "orphans" that may not related to any existing tablets
        // due to aborted schema change.
        if (is_formal_rowset) {
            std::lock_guard lock(recycled_tablets_mtx_);
            if (recycled_tablets_.count(rs.tablet_id())) {
                continue; // Rowset data has already been deleted
            }
        }

        auto it = accessor_map_.find(rs.resource_id());
        // possible if the accessor is not initilized correctly
        if (it == accessor_map_.end()) [[unlikely]] {
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
        if (num_segments <= 0) {
            metrics_context.total_recycled_num++;
            metrics_context.total_recycled_data_size += rs.total_disk_size();
            continue;
        }

        // Process inverted indexes
        std::vector<std::pair<int64_t, std::string>> index_ids;
        // default format as v1.
        InvertedIndexStorageFormatPB index_format = InvertedIndexStorageFormatPB::V1;
        int inverted_index_get_ret = 0;
        if (rs.has_tablet_schema()) {
            for (const auto& index : rs.tablet_schema().index()) {
                if (index.has_index_type() && index.index_type() == IndexType::INVERTED) {
                    index_ids.emplace_back(index.index_id(), index.index_suffix_name());
                }
            }
            if (rs.tablet_schema().has_inverted_index_storage_format()) {
                index_format = rs.tablet_schema().inverted_index_storage_format();
            }
        } else {
            if (!rs.has_index_id() || !rs.has_schema_version()) {
                LOG(WARNING) << "rowset must have either schema or schema_version and index_id, "
                                "instance_id="
                             << instance_id_ << " tablet_id=" << tablet_id
                             << " rowset_id=" << rowset_id;
                ret = -1;
                continue;
            }
            InvertedIndexInfo index_info;
            inverted_index_get_ret =
                    inverted_index_id_cache_->get(rs.index_id(), rs.schema_version(), index_info);
            TEST_SYNC_POINT_CALLBACK("InstanceRecycler::delete_rowset_data.tmp_rowset",
                                     &inverted_index_get_ret);
            if (inverted_index_get_ret == 0) {
                index_format = index_info.first;
                index_ids = index_info.second;
            } else if (inverted_index_get_ret == 1) {
                // 1. Schema kv not found means tablet has been recycled
                // Maybe some tablet recycle failed by some bugs
                // We need to delete again to double check
                // 2. Ensure this operation only deletes tablets and does not perform any operations on indexes,
                // because we are uncertain about the inverted index information.
                // If there are inverted indexes, some data might not be deleted,
                // but this is acceptable as we have made our best effort to delete the data.
                LOG_INFO(
                        "delete rowset data schema kv not found, need to delete again to double "
                        "check")
                        .tag("instance_id", instance_id_)
                        .tag("tablet_id", tablet_id)
                        .tag("rowset", rs.ShortDebugString());
                // Currently index_ids is guaranteed to be empty,
                // but we clear it again here as a safeguard against future code changes
                // that might cause index_ids to no longer be empty
                index_format = InvertedIndexStorageFormatPB::V2;
                index_ids.clear();
            } else {
                LOG(WARNING) << "failed to get schema kv for rowset, instance_id=" << instance_id_
                             << " tablet_id=" << tablet_id << " rowset_id=" << rowset_id;
                ret = -1;
                continue;
            }
        }
        if (rs.rowset_state() == RowsetStatePB::BEGIN_PARTIAL_UPDATE) {
            // if rowset state is RowsetStatePB::BEGIN_PARTIAL_UPDATE, the number of segments data
            // may be larger than num_segments field in RowsetMeta, so we need to delete the rowset's data by prefix
            rowsets_delete_by_prefix.emplace_back(rs.resource_id(), tablet_id, rs.rowset_id_v2());
            continue;
        }
        for (int64_t i = 0; i < num_segments; ++i) {
            file_paths.push_back(segment_path(tablet_id, rowset_id, i));
            if (index_format == InvertedIndexStorageFormatPB::V1) {
                for (const auto& index_id : index_ids) {
                    file_paths.push_back(inverted_index_path_v1(tablet_id, rowset_id, i,
                                                                index_id.first, index_id.second));
                }
            } else if (!index_ids.empty() || inverted_index_get_ret == 1) {
                // try to recycle inverted index v2 when get_ret == 1
                // we treat schema not found as if it has a v2 format inverted index
                // to reduce chance of data leakage
                if (inverted_index_get_ret == 1) {
                    LOG_INFO("delete rowset data schema kv not found, try to delete index file")
                            .tag("instance_id", instance_id_)
                            .tag("inverted index v2 path",
                                 inverted_index_path_v2(tablet_id, rowset_id, i));
                }
                file_paths.push_back(inverted_index_path_v2(tablet_id, rowset_id, i));
            }
        }
    }

    SyncExecutor<int> concurrent_delete_executor(_thread_pool_group.s3_producer_pool,
                                                 "delete_rowset_data",
                                                 [](const int& ret) { return ret != 0; });
    for (auto& [resource_id, file_paths] : resource_file_paths) {
        concurrent_delete_executor.add([&, rid = &resource_id, paths = &file_paths]() -> int {
            DCHECK(accessor_map_.count(*rid))
                    << "uninitilized accessor, instance_id=" << instance_id_
                    << " resource_id=" << resource_id << " path[0]=" << (*paths)[0];
            TEST_SYNC_POINT_CALLBACK("InstanceRecycler::delete_rowset_data.no_resource_id",
                                     &accessor_map_);
            if (!accessor_map_.contains(*rid)) {
                LOG_WARNING("delete rowset data accessor_map_ does not contains resouce id")
                        .tag("resource_id", resource_id)
                        .tag("instance_id", instance_id_);
                return -1;
            }
            auto& accessor = accessor_map_[*rid];
            int ret = accessor->delete_files(*paths);
            if (!ret) {
                // deduplication of different files with the same rowset id
                // 020000000000007fd045a62bc87a6587dd7ac274aa36e5a9_0.dat
                //020000000000007fd045a62bc87a6587dd7ac274aa36e5a9_0.idx
                std::set<std::string> deleted_rowset_id;

                std::for_each(
                        paths->begin(), paths->end(),
                        [&metrics_context, &rowsets, &deleted_rowset_id](const std::string& path) {
                            std::vector<std::string> str;
                            butil::SplitString(path, '/', &str);
                            std::string rowset_id;
                            if (auto pos = str.back().find('_'); pos != std::string::npos) {
                                rowset_id = str.back().substr(0, pos);
                            } else {
                                LOG(WARNING) << "failed to parse rowset_id, path=" << path;
                                return;
                            }
                            auto rs_meta = rowsets.find(rowset_id);
                            if (rs_meta != rowsets.end() &&
                                !deleted_rowset_id.contains(rowset_id)) {
                                deleted_rowset_id.emplace(rowset_id);
                                metrics_context.total_recycled_data_size +=
                                        rs_meta->second.total_disk_size();
                                segment_metrics_context_.total_recycled_num +=
                                        rs_meta->second.num_segments();
                                segment_metrics_context_.total_recycled_data_size +=
                                        rs_meta->second.total_disk_size();
                                metrics_context.total_recycled_num++;
                            }
                        });
                segment_metrics_context_.report();
                metrics_context.report();
            }
            return ret;
        });
    }
    for (const auto& [resource_id, tablet_id, rowset_id] : rowsets_delete_by_prefix) {
        LOG_INFO(
                "delete rowset {} by prefix because it's in BEGIN_PARTIAL_UPDATE state, "
                "resource_id={}, tablet_id={}, instance_id={}",
                rowset_id, resource_id, tablet_id, instance_id_);
        concurrent_delete_executor.add([&]() -> int {
            int ret = delete_rowset_data(resource_id, tablet_id, rowset_id);
            if (!ret) {
                auto rs = rowsets.at(rowset_id);
                metrics_context.total_recycled_data_size += rs.total_disk_size();
                metrics_context.total_recycled_num++;
                segment_metrics_context_.total_recycled_data_size += rs.total_disk_size();
                segment_metrics_context_.total_recycled_num += rs.num_segments();
                metrics_context.report();
                segment_metrics_context_.report();
            }
            return ret;
        });
    }

    bool finished = true;
    std::vector<int> rets = concurrent_delete_executor.when_all(&finished);
    for (int r : rets) {
        if (r != 0) {
            ret = -1;
            break;
        }
    }
    ret = finished ? ret : -1;
    return ret;
}

int InstanceRecycler::delete_rowset_data(const std::string& resource_id, int64_t tablet_id,
                                         const std::string& rowset_id) {
    auto it = accessor_map_.find(resource_id);
    if (it == accessor_map_.end()) {
        LOG_WARNING("instance has no such resource id")
                .tag("instance_id", instance_id_)
                .tag("resource_id", resource_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id);
        return -1;
    }
    auto& accessor = it->second;
    return accessor->delete_prefix(rowset_path_prefix(tablet_id, rowset_id));
}

int InstanceRecycler::scan_tablets_and_statistics(int64_t table_id, int64_t index_id,
                                                  RecyclerMetricsContext& metrics_context,
                                                  int64_t partition_id, bool is_empty_tablet) {
    std::string tablet_key_begin, tablet_key_end;

    if (partition_id > 0) {
        meta_tablet_key({instance_id_, table_id, index_id, partition_id, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id, partition_id + 1, 0}, &tablet_key_end);
    } else {
        meta_tablet_key({instance_id_, table_id, index_id, 0, 0}, &tablet_key_begin);
        meta_tablet_key({instance_id_, table_id, index_id + 1, 0, 0}, &tablet_key_end);
    }
    // for calculate the total num or bytes of recyled objects
    auto scan_and_statistics = [&, is_empty_tablet, this](std::string_view k,
                                                          std::string_view v) -> int {
        doris::TabletMetaCloudPB tablet_meta_pb;
        if (!tablet_meta_pb.ParseFromArray(v.data(), v.size())) {
            return 0;
        }
        int64_t tablet_id = tablet_meta_pb.tablet_id();

        if (!check_lazy_txn_finished(txn_kv_, instance_id_, tablet_meta_pb.tablet_id())) {
            return 0;
        }

        if (!is_empty_tablet) {
            if (scan_tablet_and_statistics(tablet_id, metrics_context) != 0) {
                return 0;
            }
            tablet_metrics_context_.total_need_recycle_num++;
        }
        return 0;
    };
    return scan_and_recycle(tablet_key_begin, tablet_key_end, std::move(scan_and_statistics),
                            [&metrics_context]() -> int {
                                metrics_context.report();
                                tablet_metrics_context_.report();
                                return 0;
                            });
}

int InstanceRecycler::scan_tablet_and_statistics(int64_t tablet_id,
                                                 RecyclerMetricsContext& metrics_context) {
    int ret = 0;
    std::map<std::string, RowsetMetaCloudPB> rowset_meta_map;
    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to recycle tablet ")
                .tag("tablet id", tablet_id)
                .tag("instance_id", instance_id_)
                .tag("reason", "failed to create txn");
        ret = -1;
    }
    GetRowsetResponse resp;
    std::string msg;
    MetaServiceCode code = MetaServiceCode::OK;
    // get rowsets in tablet
    internal_get_rowset(txn.get(), 0, std::numeric_limits<int64_t>::max() - 1, instance_id_,
                        tablet_id, code, msg, &resp);
    if (code != MetaServiceCode::OK) {
        LOG_WARNING("failed to get rowsets of tablet when recycle tablet")
                .tag("tablet id", tablet_id)
                .tag("msg", msg)
                .tag("code", code)
                .tag("instance id", instance_id_);
        ret = -1;
    }
    for (const auto& rs_meta : resp.rowset_meta()) {
        /*
        * For compatibility, we skip the loop for [0-1] here. 
        * The purpose of this loop is to delete object files,
        * and since [0-1] only has meta and doesn't have object files, 
        * skipping it doesn't affect system correctness. 
        *
        * If not skipped, the check "if (!rs_meta.has_resource_id())" below 
        * would return error -1 directly, causing the recycle operation to fail.
        *
        * [0-1] doesn't have resource id is a bug.
        * In the future, we will fix this problem, after that,
        * we can remove this if statement.
        *
        * TODO(Yukang-Lian): remove this if statement when [0-1] has resource id in the future.
        */

        if (rs_meta.end_version() == 1) {
            // Assert that [0-1] has no resource_id to make sure
            // this if statement will not be forgetted to remove
            // when the resource id bug is fixed
            DCHECK(!rs_meta.has_resource_id()) << "rs_meta" << rs_meta.ShortDebugString();
            continue;
        }
        if (!rs_meta.has_resource_id()) {
            LOG_WARNING("rowset meta does not have a resource id, impossible!")
                    .tag("rs_meta", rs_meta.ShortDebugString())
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id);
            continue;
        }
        DCHECK(rs_meta.has_resource_id()) << "rs_meta" << rs_meta.ShortDebugString();
        auto it = accessor_map_.find(rs_meta.resource_id());
        // possible if the accessor is not initilized correctly
        if (it == accessor_map_.end()) [[unlikely]] {
            LOG_WARNING(
                    "failed to find resource id when recycle tablet, skip this vault accessor "
                    "recycle process")
                    .tag("tablet id", tablet_id)
                    .tag("instance_id", instance_id_)
                    .tag("resource_id", rs_meta.resource_id())
                    .tag("rowset meta pb", rs_meta.ShortDebugString());
            continue;
        }

        metrics_context.total_need_recycle_data_size += rs_meta.total_disk_size();
        tablet_metrics_context_.total_need_recycle_data_size += rs_meta.total_disk_size();
        segment_metrics_context_.total_need_recycle_data_size += rs_meta.total_disk_size();
        segment_metrics_context_.total_need_recycle_num += rs_meta.num_segments();
    }
    return ret;
}

int InstanceRecycler::recycle_tablet(int64_t tablet_id, RecyclerMetricsContext& metrics_context) {
    LOG_INFO("begin to recycle rowsets in a dropped tablet")
            .tag("instance_id", instance_id_)
            .tag("tablet_id", tablet_id);

    if (instance_info_.has_multi_version_status() &&
        instance_info_.multi_version_status() != MultiVersionStatus::MULTI_VERSION_DISABLED) {
        return recycle_versioned_tablet(tablet_id, metrics_context);
    }

    int ret = 0;
    auto start_time = steady_clock::now();

    TEST_SYNC_POINT_RETURN_WITH_VALUE("recycle_tablet::begin", (int)0);

    // collect resource ids
    std::string rs_key0 = meta_rowset_key({instance_id_, tablet_id, 0});
    std::string rs_key1 = meta_rowset_key({instance_id_, tablet_id + 1, 0});
    std::string recyc_rs_key0 = recycle_rowset_key({instance_id_, tablet_id, ""});
    std::string recyc_rs_key1 = recycle_rowset_key({instance_id_, tablet_id + 1, ""});
    std::string restore_job_rs_key0 = job_restore_rowset_key({instance_id_, tablet_id, 0});
    std::string restore_job_rs_key1 = job_restore_rowset_key({instance_id_, tablet_id + 1, 0});

    std::set<std::string> resource_ids;
    int64_t recycle_rowsets_number = 0;
    int64_t recycle_segments_number = 0;
    int64_t recycle_rowsets_data_size = 0;
    int64_t recycle_rowsets_index_size = 0;
    int64_t recycle_restore_job_rowsets_number = 0;
    int64_t recycle_restore_job_segments_number = 0;
    int64_t recycle_restore_job_rowsets_data_size = 0;
    int64_t recycle_restore_job_rowsets_index_size = 0;
    int64_t max_rowset_version = 0;
    int64_t min_rowset_creation_time = INT64_MAX;
    int64_t max_rowset_creation_time = 0;
    int64_t min_rowset_expiration_time = INT64_MAX;
    int64_t max_rowset_expiration_time = 0;

    DORIS_CLOUD_DEFER {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle the rowsets of dropped tablet finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("recycle rowsets number", recycle_rowsets_number)
                .tag("recycle segments number", recycle_segments_number)
                .tag("all rowsets recycle data size", recycle_rowsets_data_size)
                .tag("all rowsets recycle index size", recycle_rowsets_index_size)
                .tag("recycle restore job rowsets number", recycle_restore_job_rowsets_number)
                .tag("recycle restore job segments number", recycle_restore_job_segments_number)
                .tag("all restore job rowsets recycle data size",
                     recycle_restore_job_rowsets_data_size)
                .tag("all restore job rowsets recycle index size",
                     recycle_restore_job_rowsets_index_size)
                .tag("max rowset version", max_rowset_version)
                .tag("min rowset creation time", min_rowset_creation_time)
                .tag("max rowset creation time", max_rowset_creation_time)
                .tag("min rowset expiration time", min_rowset_expiration_time)
                .tag("max rowset expiration time", max_rowset_expiration_time)
                .tag("ret", ret);
    };

    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to recycle tablet ")
                .tag("tablet id", tablet_id)
                .tag("instance_id", instance_id_)
                .tag("reason", "failed to create txn");
        ret = -1;
    }
    GetRowsetResponse resp;
    std::string msg;
    MetaServiceCode code = MetaServiceCode::OK;
    // get rowsets in tablet
    internal_get_rowset(txn.get(), 0, std::numeric_limits<int64_t>::max() - 1, instance_id_,
                        tablet_id, code, msg, &resp);
    if (code != MetaServiceCode::OK) {
        LOG_WARNING("failed to get rowsets of tablet when recycle tablet")
                .tag("tablet id", tablet_id)
                .tag("msg", msg)
                .tag("code", code)
                .tag("instance id", instance_id_);
        ret = -1;
    }
    TEST_SYNC_POINT_CALLBACK("InstanceRecycler::recycle_tablet.create_rowset_meta", &resp);

    for (const auto& rs_meta : resp.rowset_meta()) {
        /*
        * For compatibility, we skip the loop for [0-1] here. 
        * The purpose of this loop is to delete object files,
        * and since [0-1] only has meta and doesn't have object files, 
        * skipping it doesn't affect system correctness. 
        *
        * If not skipped, the check "if (!rs_meta.has_resource_id())" below 
        * would return error -1 directly, causing the recycle operation to fail.
        *
        * [0-1] doesn't have resource id is a bug.
        * In the future, we will fix this problem, after that,
        * we can remove this if statement.
        *
        * TODO(Yukang-Lian): remove this if statement when [0-1] has resource id in the future.
        */

        if (rs_meta.end_version() == 1) {
            // Assert that [0-1] has no resource_id to make sure
            // this if statement will not be forgetted to remove
            // when the resource id bug is fixed
            DCHECK(!rs_meta.has_resource_id()) << "rs_meta" << rs_meta.ShortDebugString();
            recycle_rowsets_number += 1;
            continue;
        }
        if (!rs_meta.has_resource_id()) {
            LOG_WARNING("rowset meta does not have a resource id, impossible!")
                    .tag("rs_meta", rs_meta.ShortDebugString())
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id);
            return -1;
        }
        DCHECK(rs_meta.has_resource_id()) << "rs_meta" << rs_meta.ShortDebugString();
        auto it = accessor_map_.find(rs_meta.resource_id());
        // possible if the accessor is not initilized correctly
        if (it == accessor_map_.end()) [[unlikely]] {
            LOG_WARNING(
                    "failed to find resource id when recycle tablet, skip this vault accessor "
                    "recycle process")
                    .tag("tablet id", tablet_id)
                    .tag("instance_id", instance_id_)
                    .tag("resource_id", rs_meta.resource_id())
                    .tag("rowset meta pb", rs_meta.ShortDebugString());
            return -1;
        }
        recycle_rowsets_number += 1;
        recycle_segments_number += rs_meta.num_segments();
        recycle_rowsets_data_size += rs_meta.data_disk_size();
        recycle_rowsets_index_size += rs_meta.index_disk_size();
        max_rowset_version = std::max(max_rowset_version, rs_meta.end_version());
        min_rowset_creation_time = std::min(min_rowset_creation_time, rs_meta.creation_time());
        max_rowset_creation_time = std::max(max_rowset_creation_time, rs_meta.creation_time());
        min_rowset_expiration_time = std::min(min_rowset_expiration_time, rs_meta.txn_expiration());
        max_rowset_expiration_time = std::max(max_rowset_expiration_time, rs_meta.txn_expiration());
        resource_ids.emplace(rs_meta.resource_id());
    }

    // get restore job rowset in tablet
    std::vector<std::pair<std::string, doris::RowsetMetaCloudPB>> restore_job_rs_metas;
    scan_restore_job_rowset(txn.get(), instance_id_, tablet_id, code, msg, &restore_job_rs_metas);
    if (code != MetaServiceCode::OK) {
        LOG_WARNING("scan restore job rowsets failed when recycle tablet")
                .tag("tablet id", tablet_id)
                .tag("msg", msg)
                .tag("code", code)
                .tag("instance id", instance_id_);
        return -1;
    }

    for (auto& [_, rs_meta] : restore_job_rs_metas) {
        if (!rs_meta.has_resource_id()) {
            LOG_WARNING("rowset meta does not have a resource id, impossible!")
                    .tag("rs_meta", rs_meta.ShortDebugString())
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id);
            return -1;
        }

        auto it = accessor_map_.find(rs_meta.resource_id());
        // possible if the accessor is not initilized correctly
        if (it == accessor_map_.end()) [[unlikely]] {
            LOG_WARNING(
                    "failed to find resource id when recycle tablet, skip this vault accessor "
                    "recycle process")
                    .tag("tablet id", tablet_id)
                    .tag("instance_id", instance_id_)
                    .tag("resource_id", rs_meta.resource_id())
                    .tag("rowset meta pb", rs_meta.ShortDebugString());
            return -1;
        }
        recycle_restore_job_rowsets_number += 1;
        recycle_restore_job_segments_number += rs_meta.num_segments();
        recycle_restore_job_rowsets_data_size += rs_meta.data_disk_size();
        recycle_restore_job_rowsets_index_size += rs_meta.index_disk_size();
        resource_ids.emplace(rs_meta.resource_id());
    }

    LOG_INFO("recycle tablet start to delete object")
            .tag("instance id", instance_id_)
            .tag("tablet id", tablet_id)
            .tag("recycle tablet resource ids are",
                 std::accumulate(resource_ids.begin(), resource_ids.begin(), std::string(),
                                 [](std::string rs_id, const auto& it) {
                                     return rs_id.empty() ? it : rs_id + ", " + it;
                                 }));

    SyncExecutor<int> concurrent_delete_executor(
            _thread_pool_group.s3_producer_pool,
            fmt::format("delete tablet {} s3 rowset", tablet_id),
            [](const int& ret) { return ret != 0; });

    // delete all rowset data in this tablet
    // ATTN: there may be data leak if not all accessor initilized successfully
    //       partial data deleted if the tablet is stored cross-storage vault
    //       vault id is not attached to TabletMeta...
    for (const auto& resource_id : resource_ids) {
        concurrent_delete_executor.add([&, rs_id = resource_id,
                                        accessor_ptr = accessor_map_[resource_id]]() {
            std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&](int*) {
                g_bvar_recycler_vault_recycle_task_concurrency.put(
                        {instance_id_, metrics_context.operation_type, rs_id}, -1);
                metrics_context.report();
            });
            g_bvar_recycler_vault_recycle_task_concurrency.put(
                    {instance_id_, metrics_context.operation_type, rs_id}, 1);
            int res = accessor_ptr->delete_directory(tablet_path_prefix(tablet_id));
            if (res != 0) {
                LOG(WARNING) << "failed to delete rowset data of tablet " << tablet_id
                             << " path=" << accessor_ptr->uri();
                g_bvar_recycler_vault_recycle_status.put({instance_id_, rs_id, "abnormal"}, 1);
                return -1;
            }
            g_bvar_recycler_vault_recycle_status.put({instance_id_, rs_id, "normal"}, 1);
            return 0;
        });
    }

    bool finished = true;
    std::vector<int> rets = concurrent_delete_executor.when_all(&finished);
    for (int r : rets) {
        if (r != 0) {
            ret = -1;
        }
    }

    ret = finished ? ret : -1;

    if (ret != 0) { // failed recycle tablet data
        LOG_WARNING("ret!=0")
                .tag("finished", finished)
                .tag("ret", ret)
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id);
        return ret;
    }

    tablet_metrics_context_.total_recycled_data_size +=
            recycle_rowsets_data_size + recycle_rowsets_index_size;
    tablet_metrics_context_.total_recycled_num += 1;
    segment_metrics_context_.total_recycled_num += recycle_segments_number;
    segment_metrics_context_.total_recycled_data_size +=
            recycle_rowsets_data_size + recycle_rowsets_index_size;
    metrics_context.total_recycled_data_size +=
            recycle_rowsets_data_size + recycle_rowsets_index_size;
    tablet_metrics_context_.report();
    segment_metrics_context_.report();
    metrics_context.report();

    txn.reset();
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to recycle tablet ")
                .tag("tablet id", tablet_id)
                .tag("instance_id", instance_id_)
                .tag("reason", "failed to create txn");
        ret = -1;
    }
    // delete all rowset kv in this tablet
    txn->remove(rs_key0, rs_key1);
    txn->remove(recyc_rs_key0, recyc_rs_key1);
    txn->remove(restore_job_rs_key0, restore_job_rs_key1);

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

    if (ret == 0) {
        // All object files under tablet have been deleted
        std::lock_guard lock(recycled_tablets_mtx_);
        recycled_tablets_.insert(tablet_id);
    }

    return ret;
}

int InstanceRecycler::recycle_versioned_tablet(int64_t tablet_id,
                                               RecyclerMetricsContext& metrics_context) {
    int ret = 0;
    auto start_time = steady_clock::now();

    TEST_SYNC_POINT_RETURN_WITH_VALUE("recycle_tablet::begin", (int)0);

    // collect resource ids
    std::string rs_key0 = meta_rowset_key({instance_id_, tablet_id, 0});
    std::string rs_key1 = meta_rowset_key({instance_id_, tablet_id + 1, 0});
    std::string recyc_rs_key0 = recycle_rowset_key({instance_id_, tablet_id, ""});
    std::string recyc_rs_key1 = recycle_rowset_key({instance_id_, tablet_id + 1, ""});

    int64_t recycle_rowsets_number = 0;
    int64_t recycle_segments_number = 0;
    int64_t recycle_rowsets_data_size = 0;
    int64_t recycle_rowsets_index_size = 0;
    int64_t max_rowset_version = 0;
    int64_t min_rowset_creation_time = INT64_MAX;
    int64_t max_rowset_creation_time = 0;
    int64_t min_rowset_expiration_time = INT64_MAX;
    int64_t max_rowset_expiration_time = 0;

    DORIS_CLOUD_DEFER {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG_INFO("recycle the rowsets of dropped tablet finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("recycle rowsets number", recycle_rowsets_number)
                .tag("recycle segments number", recycle_segments_number)
                .tag("all rowsets recycle data size", recycle_rowsets_data_size)
                .tag("all rowsets recycle index size", recycle_rowsets_index_size)
                .tag("max rowset version", max_rowset_version)
                .tag("min rowset creation time", min_rowset_creation_time)
                .tag("max rowset creation time", max_rowset_creation_time)
                .tag("min rowset expiration time", min_rowset_expiration_time)
                .tag("max rowset expiration time", max_rowset_expiration_time)
                .tag("ret", ret);
    };

    std::unique_ptr<Transaction> txn;
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to recycle tablet ")
                .tag("tablet id", tablet_id)
                .tag("instance_id", instance_id_)
                .tag("reason", "failed to create txn");
        ret = -1;
    }
    GetRowsetResponse resp;
    std::string msg;
    MetaServiceCode code = MetaServiceCode::OK;
    // get rowsets in tablet
    internal_get_rowset(txn.get(), 0, std::numeric_limits<int64_t>::max() - 1, instance_id_,
                        tablet_id, code, msg, &resp);
    if (code != MetaServiceCode::OK) {
        LOG_WARNING("failed to get rowsets of tablet when recycle tablet")
                .tag("tablet id", tablet_id)
                .tag("msg", msg)
                .tag("code", code)
                .tag("instance id", instance_id_);
        ret = -1;
    }
    TEST_SYNC_POINT_CALLBACK("InstanceRecycler::recycle_tablet.create_rowset_meta", &resp);

    SyncExecutor<int> concurrent_delete_executor(
            _thread_pool_group.s3_producer_pool,
            fmt::format("delete tablet {} s3 rowset", tablet_id),
            [](const int& ret) { return ret != 0; });

    for (const auto& rs_meta : resp.rowset_meta()) {
        recycle_rowsets_number += 1;
        recycle_segments_number += rs_meta.num_segments();
        recycle_rowsets_data_size += rs_meta.data_disk_size();
        recycle_rowsets_index_size += rs_meta.index_disk_size();
        max_rowset_version = std::max(max_rowset_version, rs_meta.end_version());
        min_rowset_creation_time = std::min(min_rowset_creation_time, rs_meta.creation_time());
        max_rowset_creation_time = std::max(max_rowset_creation_time, rs_meta.creation_time());
        min_rowset_expiration_time = std::min(min_rowset_expiration_time, rs_meta.txn_expiration());
        max_rowset_expiration_time = std::max(max_rowset_expiration_time, rs_meta.txn_expiration());

        concurrent_delete_executor.add([tablet_id, rs_meta_pb = rs_meta, this]() {
            std::string rowset_key =
                    meta_rowset_key({instance_id_, tablet_id, rs_meta_pb.end_version()});
            return recycle_rowset_meta_and_data(rowset_key, rs_meta_pb);
        });
    }

    auto handle_recycle_rowset_kv = [&](std::string_view k, std::string_view v) {
        RecycleRowsetPB recycle_rowset;
        if (!recycle_rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle rowset").tag("key", hex(k));
            return -1;
        }
        if (!recycle_rowset.has_type()) { // compatible with old version `RecycleRowsetPB`
            if (!recycle_rowset.has_resource_id()) [[unlikely]] { // impossible
                // in old version, keep this key-value pair and it needs to be checked manually
                LOG_WARNING("rowset meta has empty resource id").tag("key", hex(k));
                return -1;
            }
            if (recycle_rowset.resource_id().empty()) [[unlikely]] {
                // old version `RecycleRowsetPB` may has empty resource_id, just remove the kv.
                LOG(INFO) << "delete the recycle rowset kv that has empty resource_id, key="
                          << hex(k) << " value=" << proto_to_json(recycle_rowset);
                return -1;
            }
            // decode rowset_id
            auto k1 = k;
            k1.remove_prefix(1);
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            decode_key(&k1, &out);
            // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id} -> RecycleRowsetPB
            const auto& rowset_id = std::get<std::string>(std::get<0>(out[4]));
            LOG_INFO("delete rowset data")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id)
                    .tag("rowset_id", rowset_id);

            concurrent_delete_executor.add(
                    [tablet_id, resource_id = recycle_rowset.resource_id(), rowset_id, this]() {
                        // delete by prefix, the recycle rowset key will be deleted by range later.
                        return delete_rowset_data(resource_id, tablet_id, rowset_id);
                    });
        } else {
            concurrent_delete_executor.add(
                    [k = std::string(k), recycle_rowset = std::move(recycle_rowset), this]() {
                        return recycle_rowset_meta_and_data(k, recycle_rowset.rowset_meta());
                    });
        }
        return 0;
    };

    if (scan_and_recycle(recyc_rs_key0, recyc_rs_key1, std::move(handle_recycle_rowset_kv))) {
        LOG_WARNING("failed to recycle rowset kv of tablet")
                .tag("tablet id", tablet_id)
                .tag("instance_id", instance_id_)
                .tag("reason", "failed to scan and recycle RecycleRowsetPB");
        ret = -1;
    }

    bool finished = true;
    std::vector<int> rets = concurrent_delete_executor.when_all(&finished);
    for (int r : rets) {
        if (r != 0) {
            ret = -1;
        }
    }

    ret = finished ? ret : -1;

    if (ret != 0) { // failed recycle tablet data
        LOG_WARNING("ret!=0")
                .tag("finished", finished)
                .tag("ret", ret)
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id);
        return ret;
    }

    tablet_metrics_context_.total_recycled_data_size +=
            recycle_rowsets_data_size + recycle_rowsets_index_size;
    tablet_metrics_context_.total_recycled_num += 1;
    segment_metrics_context_.total_recycled_num += recycle_segments_number;
    segment_metrics_context_.total_recycled_data_size +=
            recycle_rowsets_data_size + recycle_rowsets_index_size;
    metrics_context.total_recycled_data_size +=
            recycle_rowsets_data_size + recycle_rowsets_index_size;
    tablet_metrics_context_.report();
    segment_metrics_context_.report();
    metrics_context.report();

    txn.reset();
    if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to recycle tablet ")
                .tag("tablet id", tablet_id)
                .tag("instance_id", instance_id_)
                .tag("reason", "failed to create txn");
        ret = -1;
    }
    // delete all rowset kv in this tablet
    txn->remove(rs_key0, rs_key1);
    txn->remove(recyc_rs_key0, recyc_rs_key1);

    // remove delete bitmap for MoW table
    std::string pending_key = meta_pending_delete_bitmap_key({instance_id_, tablet_id});
    txn->remove(pending_key);
    std::string delete_bitmap_start = meta_delete_bitmap_key({instance_id_, tablet_id, "", 0, 0});
    std::string delete_bitmap_end = meta_delete_bitmap_key({instance_id_, tablet_id + 1, "", 0, 0});
    txn->remove(delete_bitmap_start, delete_bitmap_end);

    std::string versioned_idx_key = versioned::tablet_index_key({instance_id_, tablet_id});
    std::string tablet_index_val;
    TxnErrorCode err = txn->get(versioned_idx_key, &tablet_index_val);
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND && err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get tablet index kv")
                .tag("instance_id", instance_id_)
                .tag("tablet_id", tablet_id)
                .tag("err", err);
        ret = -1;
    } else if (err == TxnErrorCode::TXN_OK) {
        // If the tablet index kv exists, we need to delete it
        TabletIndexPB tablet_index_pb;
        if (!tablet_index_pb.ParseFromString(tablet_index_val)) {
            LOG_WARNING("failed to parse tablet index pb")
                    .tag("instance_id", instance_id_)
                    .tag("tablet_id", tablet_id);
            ret = -1;
        } else {
            std::string versioned_inverted_idx_key = versioned::tablet_inverted_index_key(
                    {instance_id_, tablet_index_pb.db_id(), tablet_index_pb.table_id(),
                     tablet_index_pb.index_id(), tablet_index_pb.partition_id(), tablet_id});
            txn->remove(versioned_inverted_idx_key);
            txn->remove(versioned_idx_key);
        }
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to delete rowset kv of tablet " << tablet_id << ", err=" << err;
        ret = -1;
    }

    if (ret == 0) {
        // All object files under tablet have been deleted
        std::lock_guard lock(recycled_tablets_mtx_);
        recycled_tablets_.insert(tablet_id);
    }

    return ret;
}

int InstanceRecycler::recycle_rowsets() {
    if (instance_info_.has_multi_version_status() &&
        instance_info_.multi_version_status() != MultiVersionStatus::MULTI_VERSION_DISABLED) {
        return recycle_versioned_rowsets();
    }

    const std::string task_name = "recycle_rowsets";
    int64_t num_scanned = 0;
    int64_t num_expired = 0;
    int64_t num_prepare = 0;
    int64_t num_compacted = 0;
    int64_t num_empty_rowset = 0;
    size_t total_rowset_key_size = 0;
    size_t total_rowset_value_size = 0;
    size_t expired_rowset_size = 0;
    std::atomic_long num_recycled = 0;
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id_, 0, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id_, INT64_MAX, ""};
    std::string recyc_rs_key0;
    std::string recyc_rs_key1;
    recycle_rowset_key(recyc_rs_key_info0, &recyc_rs_key0);
    recycle_rowset_key(recyc_rs_key_info1, &recyc_rs_key1);

    LOG_WARNING("begin to recycle rowsets").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled)
                .tag("num_recycled.prepare", num_prepare)
                .tag("num_recycled.compacted", num_compacted)
                .tag("num_recycled.empty_rowset", num_empty_rowset)
                .tag("total_rowset_meta_key_size_scanned", total_rowset_key_size)
                .tag("total_rowset_meta_value_size_scanned", total_rowset_value_size)
                .tag("expired_rowset_meta_size", expired_rowset_size);
    };

    std::vector<std::string> rowset_keys;
    // rowset_id -> rowset_meta
    // store rowset id and meta for statistics rs size when delete
    std::map<std::string, doris::RowsetMetaCloudPB> rowsets;

    // Store keys of rowset recycled by background workers
    std::mutex async_recycled_rowset_keys_mutex;
    std::vector<std::string> async_recycled_rowset_keys;
    auto worker_pool = std::make_unique<SimpleThreadPool>(
            config::instance_recycler_worker_pool_size, "recycle_rowsets");
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

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    auto handle_rowset_kv = [&](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        total_rowset_key_size += k.size();
        total_rowset_value_size += v.size();
        RecycleRowsetPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle rowset").tag("key", hex(k));
            return -1;
        }

        int64_t current_time = ::time(nullptr);
        int64_t expiration = calculate_rowset_expired_time(instance_id_, rowset, &earlest_ts);

        VLOG_DEBUG << "recycle rowset scan, key=" << hex(k) << " num_scanned=" << num_scanned
                   << " num_expired=" << num_expired << " expiration=" << expiration
                   << " RecycleRowsetPB=" << rowset.ShortDebugString();
        if (current_time < expiration) { // not expired
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
                rowset_keys.emplace_back(k);
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
                  << " rowset_meta_size=" << v.size()
                  << " creation_time=" << rowset_meta->creation_time();
        if (rowset.type() == RecycleRowsetPB::PREPARE) {
            // unable to calculate file path, can only be deleted by rowset id prefix
            num_prepare += 1;
            if (delete_rowset_data_by_prefix(std::string(k), rowset_meta->resource_id(),
                                             rowset_meta->tablet_id(),
                                             rowset_meta->rowset_id_v2()) != 0) {
                return -1;
            }
        } else {
            num_compacted += rowset.type() == RecycleRowsetPB::COMPACT;
            rowset_keys.emplace_back(k);
            rowsets.emplace(rowset_meta->rowset_id_v2(), std::move(*rowset_meta));
            if (rowset_meta->num_segments() <= 0) { // Skip empty rowset
                ++num_empty_rowset;
            }
        }
        return 0;
    };

    auto loop_done = [&]() -> int {
        std::vector<std::string> rowset_keys_to_delete;
        // rowset_id -> rowset_meta
        // store rowset id and meta for statistics rs size when delete
        std::map<std::string, doris::RowsetMetaCloudPB> rowsets_to_delete;
        rowset_keys_to_delete.swap(rowset_keys);
        rowsets_to_delete.swap(rowsets);
        worker_pool->submit([&, rowset_keys_to_delete = std::move(rowset_keys_to_delete),
                             rowsets_to_delete = std::move(rowsets_to_delete)]() {
            if (delete_rowset_data(rowsets_to_delete, RowsetRecyclingState::FORMAL_ROWSET,
                                   metrics_context) != 0) {
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

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_rowsets();
    }
    // recycle_func and loop_done for scan and recycle
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

int InstanceRecycler::recycle_restore_jobs() {
    const std::string task_name = "recycle_restore_jobs";
    int64_t num_scanned = 0;
    int64_t num_expired = 0;
    int64_t num_recycled = 0;
    int64_t num_aborted = 0;

    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    JobRestoreTabletKeyInfo restore_job_key_info0 {instance_id_, 0};
    JobRestoreTabletKeyInfo restore_job_key_info1 {instance_id_, INT64_MAX};
    std::string restore_job_key0;
    std::string restore_job_key1;
    job_restore_tablet_key(restore_job_key_info0, &restore_job_key0);
    job_restore_tablet_key(restore_job_key_info1, &restore_job_key1);

    LOG_INFO("begin to recycle restore jobs").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();

        LOG_INFO("recycle restore jobs finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled)
                .tag("num_aborted", num_aborted);
    };

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    std::vector<std::string_view> restore_job_keys;
    auto recycle_func = [&, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RestoreJobCloudPB restore_job_pb;
        if (!restore_job_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle partition value").tag("key", hex(k));
            return -1;
        }
        int64_t expiration =
                calculate_restore_job_expired_time(instance_id_, restore_job_pb, &earlest_ts);
        VLOG_DEBUG << "recycle restore job scan, key=" << hex(k) << " num_scanned=" << num_scanned
                   << " num_expired=" << num_expired << " expiration time=" << expiration
                   << " job expiration=" << restore_job_pb.expired_at_s()
                   << " ctime=" << restore_job_pb.ctime_s() << " mtime=" << restore_job_pb.mtime_s()
                   << " state=" << restore_job_pb.state();
        int64_t current_time = ::time(nullptr);
        if (current_time < expiration) { // not expired
            return 0;
        }
        ++num_expired;

        int64_t tablet_id = restore_job_pb.tablet_id();
        LOG(INFO) << "begin to recycle expired restore jobs, instance_id=" << instance_id_
                  << " restore_job_pb=" << restore_job_pb.DebugString();

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to recycle restore job")
                    .tag("err", err)
                    .tag("tablet id", tablet_id)
                    .tag("instance_id", instance_id_)
                    .tag("reason", "failed to create txn");
            return -1;
        }

        std::string val;
        err = txn->get(k, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) { // maybe recycled, skip it
            LOG_INFO("restore job {} has been recycled", tablet_id);
            return 0;
        }
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to get kv");
            return -1;
        }
        restore_job_pb.Clear();
        if (!restore_job_pb.ParseFromString(val)) {
            LOG_WARNING("malformed recycle restore job value").tag("key", hex(k));
            return -1;
        }

        // PREPARED or COMMITTED, change state to DROPPED and return
        if (restore_job_pb.state() == RestoreJobCloudPB::PREPARED ||
            restore_job_pb.state() == RestoreJobCloudPB::COMMITTED) {
            restore_job_pb.set_state(RestoreJobCloudPB::DROPPED);
            restore_job_pb.set_need_recycle_data(true);
            txn->put(k, restore_job_pb.SerializeAsString());
            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to commit txn: {}", err);
                return -1;
            }
            num_aborted++;
            return 0;
        }

        // Change state to RECYCLING
        if (restore_job_pb.state() != RestoreJobCloudPB::RECYCLING) {
            restore_job_pb.set_state(RestoreJobCloudPB::RECYCLING);
            txn->put(k, restore_job_pb.SerializeAsString());
            err = txn->commit();
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to commit txn: {}", err);
                return -1;
            }
        }

        std::string restore_job_rs_key0 = job_restore_rowset_key({instance_id_, tablet_id, 0});
        std::string restore_job_rs_key1 = job_restore_rowset_key({instance_id_, tablet_id + 1, 0});

        // Recycle all data associated with the restore job.
        // This includes rowsets, segments, and related resources.
        bool need_recycle_data = restore_job_pb.need_recycle_data();
        if (need_recycle_data && recycle_tablet(tablet_id, metrics_context) != 0) {
            LOG_WARNING("failed to recycle tablet")
                    .tag("tablet_id", tablet_id)
                    .tag("instance_id", instance_id_);
            return -1;
        }

        // delete all restore job rowset kv
        txn->remove(restore_job_rs_key0, restore_job_rs_key1);

        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to recycle tablet restore job rowset kv")
                    .tag("err", err)
                    .tag("tablet id", tablet_id)
                    .tag("instance_id", instance_id_)
                    .tag("reason", "failed to commit txn");
            return -1;
        }

        metrics_context.total_recycled_num = ++num_recycled;
        metrics_context.report();
        check_recycle_task(instance_id_, task_name, num_scanned, num_recycled, start_time);
        restore_job_keys.push_back(k);

        LOG(INFO) << "finish to recycle expired restore job, key=" << hex(k)
                  << " tablet_id=" << tablet_id;
        return 0;
    };

    auto loop_done = [&restore_job_keys, this]() -> int {
        if (restore_job_keys.empty()) return 0;
        DORIS_CLOUD_DEFER {
            restore_job_keys.clear();
        };

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to recycle restore job")
                    .tag("err", err)
                    .tag("instance_id", instance_id_)
                    .tag("reason", "failed to create txn");
            return -1;
        }
        for (auto& k : restore_job_keys) {
            txn->remove(k);
        }
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to recycle restore job")
                    .tag("err", err)
                    .tag("instance_id", instance_id_)
                    .tag("reason", "failed to commit txn");
            return -1;
        }
        return 0;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_restore_jobs();
    }

    return scan_and_recycle(restore_job_key0, restore_job_key1, std::move(recycle_func),
                            std::move(loop_done));
}

int InstanceRecycler::recycle_versioned_rowsets() {
    const std::string task_name = "recycle_rowsets";
    int64_t num_scanned = 0;
    int64_t num_expired = 0;
    int64_t num_prepare = 0;
    int64_t num_compacted = 0;
    int64_t num_empty_rowset = 0;
    size_t total_rowset_key_size = 0;
    size_t total_rowset_value_size = 0;
    size_t expired_rowset_size = 0;
    std::atomic_long num_recycled = 0;
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id_, 0, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id_, INT64_MAX, ""};
    std::string recyc_rs_key0;
    std::string recyc_rs_key1;
    recycle_rowset_key(recyc_rs_key_info0, &recyc_rs_key0);
    recycle_rowset_key(recyc_rs_key_info1, &recyc_rs_key1);

    LOG_WARNING("begin to recycle rowsets").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled)
                .tag("num_recycled.prepare", num_prepare)
                .tag("num_recycled.compacted", num_compacted)
                .tag("num_recycled.empty_rowset", num_empty_rowset)
                .tag("total_rowset_meta_key_size_scanned", total_rowset_key_size)
                .tag("total_rowset_meta_value_size_scanned", total_rowset_value_size)
                .tag("expired_rowset_meta_size", expired_rowset_size);
    };

    std::vector<std::string> orphan_rowset_keys;

    // Store keys of rowset recycled by background workers
    std::mutex async_recycled_rowset_keys_mutex;
    std::vector<std::string> async_recycled_rowset_keys;
    auto worker_pool = std::make_unique<SimpleThreadPool>(
            config::instance_recycler_worker_pool_size, "recycle_rowsets");
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
                    // The async recycled rowsets are staled format or has not been used,
                    // so we don't need to check the rowset ref count key.
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
        orphan_rowset_keys.push_back(std::move(key));
        return 0;
    };

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    auto handle_rowset_kv = [&, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        total_rowset_key_size += k.size();
        total_rowset_value_size += v.size();
        RecycleRowsetPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle rowset").tag("key", hex(k));
            return -1;
        }

        int final_expiration = calculate_rowset_expired_time(instance_id_, rowset, &earlest_ts);

        VLOG_DEBUG << "recycle rowset scan, key=" << hex(k) << " num_scanned=" << num_scanned
                   << " num_expired=" << num_expired << " expiration=" << final_expiration
                   << " RecycleRowsetPB=" << rowset.ShortDebugString();
        int64_t current_time = ::time(nullptr);
        if (current_time < final_expiration) { // not expired
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
                orphan_rowset_keys.emplace_back(k);
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
                  << " rowset_meta_size=" << v.size()
                  << " creation_time=" << rowset_meta->creation_time();
        if (rowset.type() == RecycleRowsetPB::PREPARE) {
            // unable to calculate file path, can only be deleted by rowset id prefix
            num_prepare += 1;
            if (delete_rowset_data_by_prefix(std::string(k), rowset_meta->resource_id(),
                                             rowset_meta->tablet_id(),
                                             rowset_meta->rowset_id_v2()) != 0) {
                return -1;
            }
        } else {
            bool is_compacted = rowset.type() == RecycleRowsetPB::COMPACT;
            worker_pool->submit(
                    [&, is_compacted, k = std::string(k), rowset_meta = std::move(*rowset_meta)]() {
                        if (recycle_rowset_meta_and_data(k, rowset_meta) != 0) {
                            return;
                        }
                        num_compacted += is_compacted;
                        num_recycled.fetch_add(1, std::memory_order_relaxed);
                        if (rowset_meta.num_segments() == 0) {
                            ++num_empty_rowset;
                        }
                    });
        }
        return 0;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_rowsets();
    }

    auto loop_done = [&]() -> int {
        if (txn_remove(txn_kv_.get(), orphan_rowset_keys)) {
            LOG(WARNING) << "failed to delete recycle rowset kv, instance_id=" << instance_id_;
        }
        orphan_rowset_keys.clear();
        return 0;
    };

    // recycle_func and loop_done for scan and recycle
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

int InstanceRecycler::recycle_rowset_meta_and_data(std::string_view rowset_meta_key,
                                                   const RowsetMetaCloudPB& rowset_meta) {
    constexpr int MAX_RETRY = 10;
    int64_t tablet_id = rowset_meta.tablet_id();
    const std::string& rowset_id = rowset_meta.rowset_id_v2();
    for (int i = 0; i < MAX_RETRY; ++i) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn").tag("err", err);
            return -1;
        }

        std::string rowset_ref_count_key =
                versioned::data_rowset_ref_count_key({instance_id_, tablet_id, rowset_id});
        int64_t ref_count = 0;
        {
            std::string value;
            TxnErrorCode err = txn->get(rowset_ref_count_key, &value);
            if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
                // This is the old version rowset, we could recycle it directly.
                ref_count = 1;
            } else if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to get rowset ref count key").tag("err", err);
                return -1;
            } else if (!txn->decode_atomic_int(value, &ref_count)) {
                LOG_WARNING("failed to decode rowset data ref count")
                        .tag("key", hex(rowset_meta_key))
                        .tag("rowset_id", rowset_id)
                        .tag("tablet_id", tablet_id)
                        .tag("value", hex(value));
                return -1;
            }
        };

        if (ref_count == 1) {
            // It would not be added since it is recycling.
            if (delete_rowset_data(rowset_meta) != 0) {
                LOG_WARNING("failed to delete rowset data")
                        .tag("tablet_id", tablet_id)
                        .tag("rowset_id", rowset_id)
                        .tag("key", hex(rowset_meta_key));
                return -1;
            }

            // Reset the transaction to avoid timeout.
            err = txn_kv_->create_txn(&txn);
            if (err != TxnErrorCode::TXN_OK) {
                LOG_WARNING("failed to create txn").tag("err", err);
                return -1;
            }
            txn->remove(rowset_ref_count_key);
            LOG_INFO("delete rowset data ref count key")
                    .tag("txn_id", rowset_meta.txn_id())
                    .tag("key", hex(rowset_meta_key))
                    .tag("tablet_id", tablet_id)
                    .tag("rowset_id", rowset_id);
        } else {
            // Decrease the rowset ref count.
            //
            // The read conflict range will protect the rowset ref count key, if any conflict happens,
            // we will retry and check whether the rowset ref count is 1 and the data need to be deleted.
            txn->atomic_add(rowset_ref_count_key, -1);
            LOG_INFO("decrease rowset data ref count")
                    .tag("txn_id", rowset_meta.txn_id())
                    .tag("key", hex(rowset_meta_key))
                    .tag("tablet_id", tablet_id)
                    .tag("rowset_id", rowset_id)
                    .tag("ref_count", ref_count - 1);
        }

        txn->remove(rowset_meta_key);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_CONFLICT) { // unlikely
            // The rowset ref count key has been changed, we need to retry.
            VLOG_DEBUG << "decrease rowset ref count but txn conflict, retry"
                       << "key=" << hex(rowset_meta_key) << " tablet_id=" << tablet_id
                       << " rowset_id=" << rowset_id << ", ref_count=" << ref_count
                       << ", retry=" << i;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        } else if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to recycle rowset meta and data")
                    .tag("key", hex(rowset_meta_key))
                    .tag("err", err);
            return -1;
        }
        LOG_INFO("recycle rowset meta and data success")
                .tag("key", hex(rowset_meta_key))
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id);
        return 0;
    }
    LOG_WARNING("failed to recycle rowset meta and data after retry")
            .tag("key", hex(rowset_meta_key))
            .tag("tablet_id", tablet_id)
            .tag("rowset_id", rowset_id)
            .tag("retry", MAX_RETRY);
    return -1;
}

int InstanceRecycler::recycle_tmp_rowsets() {
    const std::string task_name = "recycle_tmp_rowsets";
    int64_t num_scanned = 0;
    int64_t num_expired = 0;
    int64_t num_recycled = 0;
    size_t expired_rowset_size = 0;
    size_t total_rowset_key_size = 0;
    size_t total_rowset_value_size = 0;
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    MetaRowsetTmpKeyInfo tmp_rs_key_info0 {instance_id_, 0, 0};
    MetaRowsetTmpKeyInfo tmp_rs_key_info1 {instance_id_, INT64_MAX, 0};
    std::string tmp_rs_key0;
    std::string tmp_rs_key1;
    meta_rowset_tmp_key(tmp_rs_key_info0, &tmp_rs_key0);
    meta_rowset_tmp_key(tmp_rs_key_info1, &tmp_rs_key1);

    LOG_WARNING("begin to recycle tmp rowsets").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle tmp rowsets finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled)
                .tag("total_rowset_meta_key_size_scanned", total_rowset_key_size)
                .tag("total_rowset_meta_value_size_scanned", total_rowset_value_size)
                .tag("expired_rowset_meta_size_recycled", expired_rowset_size);
    };

    // Elements in `tmp_rowset_keys` has the same lifetime as `it`
    std::vector<std::string_view> tmp_rowset_keys;
    std::vector<std::string> tmp_rowset_ref_count_keys;
    // rowset_id -> rowset_meta
    // store tmp_rowset id and meta for statistics rs size when delete
    std::map<std::string, doris::RowsetMetaCloudPB> tmp_rowsets;

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    auto handle_rowset_kv = [&num_scanned, &num_expired, &tmp_rowset_keys, &tmp_rowsets,
                             &expired_rowset_size, &total_rowset_key_size, &total_rowset_value_size,
                             &earlest_ts, &tmp_rowset_ref_count_keys,
                             this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        total_rowset_key_size += k.size();
        total_rowset_value_size += v.size();
        doris::RowsetMetaCloudPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed rowset meta").tag("key", hex(k));
            return -1;
        }
        int64_t expiration = calculate_tmp_rowset_expired_time(instance_id_, rowset, &earlest_ts);
        VLOG_DEBUG << "recycle tmp rowset scan, key=" << hex(k) << " num_scanned=" << num_scanned
                   << " num_expired=" << num_expired << " expiration=" << expiration
                   << " txn_expiration=" << rowset.txn_expiration()
                   << " rowset_creation_time=" << rowset.creation_time();
        int64_t current_time = ::time(nullptr);
        if (current_time < expiration) { // not expired
            return 0;
        }

        DCHECK_GT(rowset.txn_id(), 0)
                << "txn_id=" << rowset.txn_id() << " rowset=" << rowset.ShortDebugString();
        if (!is_txn_finished(txn_kv_, instance_id_, rowset.txn_id())) {
            LOG(INFO) << "txn is not finished, skip recycle tmp rowset, instance_id="
                      << instance_id_ << " tablet_id=" << rowset.tablet_id()
                      << " rowset_id=" << rowset.rowset_id_v2() << " version=["
                      << rowset.start_version() << '-' << rowset.end_version()
                      << "] txn_id=" << rowset.txn_id()
                      << " creation_time=" << rowset.creation_time() << " expiration=" << expiration
                      << " txn_expiration=" << rowset.txn_expiration();
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
                  << " creation_time=" << rowset.creation_time() << " num_scanned=" << num_scanned
                  << " num_expired=" << num_expired;

        tmp_rowset_keys.push_back(k);
        // Remove the rowset ref count key directly since it has not been used.
        std::string rowset_ref_count_key = versioned::data_rowset_ref_count_key(
                {instance_id_, rowset.tablet_id(), rowset.rowset_id_v2()});
        LOG(INFO) << "delete rowset ref count key, instance_id=" << instance_id_
                  << "key=" << hex(rowset_ref_count_key);
        tmp_rowset_ref_count_keys.push_back(rowset_ref_count_key);

        tmp_rowsets.emplace(rowset.rowset_id_v2(), std::move(rowset));
        return 0;
    };

    auto loop_done = [&tmp_rowset_keys, &tmp_rowsets, &num_recycled, &metrics_context,
                      &tmp_rowset_ref_count_keys, this]() -> int {
        DORIS_CLOUD_DEFER {
            tmp_rowset_keys.clear();
            tmp_rowsets.clear();
            tmp_rowset_ref_count_keys.clear();
        };
        if (delete_rowset_data(tmp_rowsets, RowsetRecyclingState::TMP_ROWSET, metrics_context) !=
            0) {
            LOG(WARNING) << "failed to delete tmp rowset data, instance_id=" << instance_id_;
            return -1;
        }
        if (txn_remove(txn_kv_.get(), tmp_rowset_keys) != 0) {
            LOG(WARNING) << "failed to delete tmp rowset kv, instance_id=" << instance_id_;
            return -1;
        }
        if (txn_remove(txn_kv_.get(), tmp_rowset_ref_count_keys) != 0) {
            LOG(WARNING) << "failed to delete tmp rowset ref count kv, instance_id="
                         << instance_id_;
            return -1;
        }
        num_recycled += tmp_rowset_keys.size();
        return 0;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_tmp_rowsets();
    }
    // recycle_func and loop_done for scan and recycle
    return scan_and_recycle(tmp_rs_key0, tmp_rs_key1, std::move(handle_rowset_kv),
                            std::move(loop_done));
}

int InstanceRecycler::scan_and_recycle(
        std::string begin, std::string_view end,
        std::function<int(std::string_view k, std::string_view v)> recycle_func,
        std::function<int()> loop_done) {
    LOG(INFO) << "begin scan_and_recycle key_range=[" << hex(begin) << "," << hex(end) << ")";
    int ret = 0;
    int64_t cnt = 0;
    int get_range_retried = 0;
    std::string err;
    DORIS_CLOUD_DEFER_COPY(begin, end) {
        LOG(INFO) << "finish scan_and_recycle key_range=[" << hex(begin) << "," << hex(end)
                  << ") num_scanned=" << cnt << " get_range_retried=" << get_range_retried
                  << " ret=" << ret << " err=" << err;
    };

    std::unique_ptr<RangeGetIterator> it;
    do {
        if (get_range_retried > 1000) {
            err = "txn_get exceeds max retry, may not scan all keys";
            ret = -1;
            return -1;
        }
        int get_ret = txn_get(txn_kv_.get(), begin, end, it);
        if (get_ret != 0) { // txn kv may complain "Request for future version"
            LOG(WARNING) << "failed to get kv, range=[" << hex(begin) << "," << hex(end)
                         << ") num_scanned=" << cnt << " txn_get_ret=" << get_ret
                         << " get_range_retried=" << get_range_retried;
            ++get_range_retried;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue; // try again
        }
        if (!it->has_next()) {
            LOG(INFO) << "no keys in the given range=[" << hex(begin) << "," << hex(end) << ")";
            break; // scan finished
        }
        while (it->has_next()) {
            ++cnt;
            // recycle corresponding resources
            auto [k, v] = it->next();
            if (!it->has_next()) {
                begin = k;
                VLOG_DEBUG << "iterator has no more kvs. key=" << hex(k);
            }
            // if we want to continue scanning, the recycle_func should not return non-zero
            if (recycle_func(k, v) != 0) {
                err = "recycle_func error";
                ret = -1;
            }
        }
        begin.push_back('\x00'); // Update to next smallest key for iteration
        // if we want to continue scanning, the recycle_func should not return non-zero
        if (loop_done && loop_done() != 0) {
            err = "loop_done error";
            ret = -1;
        }
    } while (it->more() && !stopped());
    return ret;
}

int InstanceRecycler::abort_timeout_txn() {
    const std::string task_name = "abort_timeout_txn";
    int64_t num_scanned = 0;
    int64_t num_timeout = 0;
    int64_t num_abort = 0;
    int64_t num_advance = 0;
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    TxnRunningKeyInfo txn_running_key_info0 {instance_id_, 0, 0};
    TxnRunningKeyInfo txn_running_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_txn_running_key;
    std::string end_txn_running_key;
    txn_running_key(txn_running_key_info0, &begin_txn_running_key);
    txn_running_key(txn_running_key_info1, &end_txn_running_key);

    LOG_WARNING("begin to abort timeout txn").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("end to abort timeout txn, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_timeout", num_timeout)
                .tag("num_abort", num_abort)
                .tag("num_advance", num_advance);
    };

    int64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto handle_txn_running_kv = [&num_scanned, &num_timeout, &num_abort, &num_advance,
                                  &current_time, &metrics_context,
                                  this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;

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

        if (TxnStatusPB::TXN_STATUS_COMMITTED == txn_info.status()) {
            txn.reset();
            TEST_SYNC_POINT_CALLBACK("abort_timeout_txn::advance_last_pending_txn_id", &txn_info);
            std::shared_ptr<TxnLazyCommitTask> task =
                    txn_lazy_committer_->submit(instance_id_, txn_info.txn_id());
            std::pair<MetaServiceCode, std::string> ret = task->wait();
            if (ret.first != MetaServiceCode::OK) {
                LOG(WARNING) << "lazy commit txn failed txn_id=" << txn_id << " code=" << ret.first
                             << "msg=" << ret.second;
                return -1;
            }
            ++num_advance;
            return 0;
        } else {
            TxnRunningPB txn_running_pb;
            if (!txn_running_pb.ParseFromArray(v.data(), v.size())) {
                LOG_WARNING("malformed txn_running_pb").tag("key", hex(k));
                return -1;
            }
            if (!config::force_immediate_recycle && txn_running_pb.timeout_time() > current_time) {
                return 0;
            }
            ++num_timeout;

            DCHECK(txn_info.status() != TxnStatusPB::TXN_STATUS_VISIBLE);
            txn_info.set_status(TxnStatusPB::TXN_STATUS_ABORTED);
            txn_info.set_finish_time(current_time);
            txn_info.set_reason("timeout");
            VLOG_DEBUG << "txn_info=" << txn_info.ShortDebugString();
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
            metrics_context.total_recycled_num = ++num_abort;
            metrics_context.report();
        }

        return 0;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_abort_timeout_txn();
    }
    // recycle_func and loop_done for scan and recycle
    return scan_and_recycle(begin_txn_running_key, end_txn_running_key,
                            std::move(handle_txn_running_kv));
}

int InstanceRecycler::recycle_expired_txn_label() {
    const std::string task_name = "recycle_expired_txn_label";
    int64_t num_scanned = 0;
    int64_t num_expired = 0;
    int64_t num_recycled = 0;
    RecyclerMetricsContext metrics_context(instance_id_, task_name);
    int ret = 0;

    RecycleTxnKeyInfo recycle_txn_key_info0 {instance_id_, 0, 0};
    RecycleTxnKeyInfo recycle_txn_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_recycle_txn_key;
    std::string end_recycle_txn_key;
    recycle_txn_key(recycle_txn_key_info0, &begin_recycle_txn_key);
    recycle_txn_key(recycle_txn_key_info1, &end_recycle_txn_key);
    std::vector<std::string> recycle_txn_info_keys;

    LOG_WARNING("begin to recycle expired txn").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);
    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("end to recycle expired txn, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    };

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    SyncExecutor<int> concurrent_delete_executor(
            _thread_pool_group.s3_producer_pool,
            fmt::format("recycle expired txn label, instance id {}", instance_id_),
            [](const int& ret) { return ret != 0; });

    int64_t current_time_ms =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto handle_recycle_txn_kv = [&, this](std::string_view k, std::string_view v) -> int {
        ++num_scanned;
        RecycleTxnPB recycle_txn_pb;
        if (!recycle_txn_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed txn_running_pb").tag("key", hex(k));
            return -1;
        }
        if ((config::force_immediate_recycle) ||
            (recycle_txn_pb.has_immediate() && recycle_txn_pb.immediate()) ||
            (calculate_txn_expired_time(instance_id_, recycle_txn_pb, &earlest_ts) <=
             current_time_ms)) {
            VLOG_DEBUG << "found recycle txn, key=" << hex(k);
            num_expired++;
            recycle_txn_info_keys.emplace_back(k);
        }
        return 0;
    };

    auto delete_recycle_txn_kv = [&](const std::string& k) -> int {
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
        metrics_context.total_recycled_num = ++num_recycled;
        metrics_context.report();

        LOG(INFO) << "recycle expired txn, key=" << hex(k);
        return 0;
    };

    auto loop_done = [&]() -> int {
        DORIS_CLOUD_DEFER {
            recycle_txn_info_keys.clear();
        };
        TEST_SYNC_POINT_CALLBACK(
                "InstanceRecycler::recycle_expired_txn_label.check_recycle_txn_info_keys",
                &recycle_txn_info_keys);
        for (const auto& k : recycle_txn_info_keys) {
            concurrent_delete_executor.add([&]() {
                if (delete_recycle_txn_kv(k) != 0) {
                    LOG_WARNING("failed to delete recycle txn kv")
                            .tag("instance id", instance_id_)
                            .tag("key", hex(k));
                    return -1;
                }
                return 0;
            });
        }
        bool finished = true;
        std::vector<int> rets = concurrent_delete_executor.when_all(&finished);
        for (int r : rets) {
            if (r != 0) {
                ret = -1;
            }
        }

        ret = finished ? ret : -1;

        TEST_SYNC_POINT_CALLBACK("InstanceRecycler::recycle_expired_txn_label.failure", &ret);

        if (ret != 0) {
            LOG_WARNING("recycle txn kv ret!=0")
                    .tag("finished", finished)
                    .tag("ret", ret)
                    .tag("instance_id", instance_id_);
            return ret;
        }
        return ret;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_expired_txn_label();
    }
    // recycle_func and loop_done for scan and recycle
    return scan_and_recycle(begin_recycle_txn_key, end_recycle_txn_key,
                            std::move(handle_recycle_txn_kv), std::move(loop_done));
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
        DORIS_CLOUD_DEFER {
            paths_.clear();
            copy_file_keys_.clear();
            batch_count_++;

            LOG_WARNING("begin to delete {} internal stage objects in batch {}", paths_.size(),
                        batch_count_);
        };

        StopWatch sw;
        // TODO(yuejing): 在accessor的delete_objets的实现里可以考虑如果_paths数量不超过10个的话，就直接发10个delete objection operation而不是发post
        if (0 != accessor_->delete_files(paths_)) {
            LOG_WARNING("failed to delete {} internal stage objects in batch {} and it takes {} us",
                        paths_.size(), batch_count_, sw.elapsed_us());
            return;
        }
        LOG_WARNING("succeed to delete {} internal stage objects in batch {} and it takes {} us",
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
    int64_t num_scanned = 0;
    int64_t num_finished = 0;
    int64_t num_expired = 0;
    int64_t num_recycled = 0;
    // Used for INTERNAL stage's copy jobs to tag each batch for log trace
    uint64_t batch_count = 0;
    const std::string task_name = "recycle_copy_jobs";
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    LOG_WARNING("begin to recycle copy jobs").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle copy jobs finished, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_finished", num_finished)
                .tag("num_expired", num_expired)
                .tag("num_recycled", num_recycled);
    };

    CopyJobKeyInfo key_info0 {instance_id_, "", 0, "", 0};
    CopyJobKeyInfo key_info1 {instance_id_, "\xff", 0, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);
    std::unordered_map<std::string, std::shared_ptr<BatchObjStoreAccessor>> stage_accessor_map;
    auto recycle_func = [&start_time, &num_scanned, &num_finished, &num_expired, &num_recycled,
                         &batch_count, &stage_accessor_map, &task_name, &metrics_context,
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
                    if (!config::force_immediate_recycle &&
                        current_time < copy_job.finish_time_ms() +
                                               config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                } else {
                    // For compatibility, copy job does not contain finish time before 2.2.2, use start time
                    if (!config::force_immediate_recycle &&
                        current_time < copy_job.start_time_ms() +
                                               config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                }
            }
        } else if (copy_job.job_status() == CopyJobPB::LOADING) {
            int64_t current_time =
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            // if copy job is timeout: delete all copy file kvs and copy job kv
            if (!config::force_immediate_recycle && current_time <= copy_job.timeout_time_ms()) {
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

        metrics_context.total_recycled_num = ++num_recycled;
        metrics_context.report();
        check_recycle_task(instance_id_, task_name, num_scanned, num_recycled, start_time);
        return 0;
    };

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_copy_jobs();
    }
    // recycle_func and loop_done for scan and recycle
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
    int64_t num_scanned = 0;
    int64_t num_recycled = 0;
    const std::string task_name = "recycle_stage";
    RecyclerMetricsContext metrics_context(instance_id_, task_name);

    LOG_WARNING("begin to recycle stage").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    register_recycle_task(task_name, start_time);

    DORIS_CLOUD_DEFER {
        unregister_recycle_task(task_name);
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle stage, cost={}s", cost)
                .tag("instance_id", instance_id_)
                .tag("num_scanned", num_scanned)
                .tag("num_recycled", num_recycled);
    };

    RecycleStageKeyInfo key_info0 {instance_id_, ""};
    RecycleStageKeyInfo key_info1 {instance_id_, "\xff"};
    std::string key0 = recycle_stage_key(key_info0);
    std::string key1 = recycle_stage_key(key_info1);

    std::vector<std::string_view> stage_keys;
    auto recycle_func = [&start_time, &num_scanned, &num_recycled, &stage_keys, &metrics_context,
                         this](std::string_view k, std::string_view v) -> int {
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

        LOG_WARNING("begin to delete objects of dropped internal stage")
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
        metrics_context.total_recycled_num = ++num_recycled;
        metrics_context.report();
        check_recycle_task(instance_id_, "recycle_stage", num_scanned, num_recycled, start_time);
        stage_keys.push_back(k);
        return 0;
    };

    auto loop_done = [&stage_keys, this]() -> int {
        if (stage_keys.empty()) return 0;
        DORIS_CLOUD_DEFER {
            stage_keys.clear();
        };
        if (0 != txn_remove(txn_kv_.get(), stage_keys)) {
            LOG(WARNING) << "failed to delete recycle partition kv, instance_id=" << instance_id_;
            return -1;
        }
        return 0;
    };
    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_stage();
    }
    // recycle_func and loop_done for scan and recycle
    return scan_and_recycle(key0, key1, std::move(recycle_func), std::move(loop_done));
}

int InstanceRecycler::recycle_expired_stage_objects() {
    LOG_WARNING("begin to recycle expired stage objects").tag("instance_id", instance_id_);

    int64_t start_time = duration_cast<seconds>(steady_clock::now().time_since_epoch()).count();
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_expired_stage_objects");

    DORIS_CLOUD_DEFER {
        int64_t cost =
                duration_cast<seconds>(steady_clock::now().time_since_epoch()).count() - start_time;
        metrics_context.finish_report();
        LOG_WARNING("recycle expired stage objects, cost={}s", cost)
                .tag("instance_id", instance_id_);
    };

    int ret = 0;

    if (config::enable_recycler_stats_metrics) {
        scan_and_statistics_expired_stage_objects();
    }

    for (const auto& stage : instance_info_.stages()) {
        std::stringstream ss;
        ss << "instance_id=" << instance_id_ << ", stage_id=" << stage.stage_id() << ", user_name="
           << (stage.mysql_user_name().empty() ? "null" : stage.mysql_user_name().at(0))
           << ", user_id=" << (stage.mysql_user_id().empty() ? "null" : stage.mysql_user_id().at(0))
           << ", prefix=" << stage.obj_info().prefix();

        if (stopped()) {
            break;
        }
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
            LOG(WARNING) << "failed to init s3_conf with obj_info=" << old_obj.ShortDebugString();
            continue;
        }

        s3_conf->prefix = stage.obj_info().prefix();
        std::shared_ptr<S3Accessor> accessor;
        int ret1 = S3Accessor::create(*s3_conf, &accessor);
        if (ret1 != 0) {
            LOG(WARNING) << "failed to init s3 accessor ret=" << ret1 << " " << ss.str();
            ret = -1;
            continue;
        }

        if (s3_conf->prefix.find("/stage/") == std::string::npos) {
            LOG(WARNING) << "try to delete illegal prefix, which is catastrophic, " << ss.str();
            ret = -1;
            continue;
        }

        LOG(INFO) << "recycle expired stage objects, " << ss.str();
        int64_t expiration_time =
                duration_cast<seconds>(system_clock::now().time_since_epoch()).count() -
                config::internal_stage_objects_expire_time_second;
        if (config::force_immediate_recycle) {
            expiration_time = INT64_MAX;
        }
        ret1 = accessor->delete_all(expiration_time);
        if (ret1 != 0) {
            LOG(WARNING) << "failed to recycle expired stage objects, ret=" << ret1 << " "
                         << ss.str();
            ret = -1;
            continue;
        }
        metrics_context.total_recycled_num++;
        metrics_context.report();
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

// Scan and statistics indexes that need to be recycled
int InstanceRecycler::scan_and_statistics_indexes() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_indexes");

    RecycleIndexKeyInfo index_key_info0 {instance_id_, 0};
    RecycleIndexKeyInfo index_key_info1 {instance_id_, INT64_MAX};
    std::string index_key0;
    std::string index_key1;
    recycle_index_key(index_key_info0, &index_key0);
    recycle_index_key(index_key_info1, &index_key1);
    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    auto handle_index_kv = [&, this](std::string_view k, std::string_view v) -> int {
        RecycleIndexPB index_pb;
        if (!index_pb.ParseFromArray(v.data(), v.size())) {
            return 0;
        }
        int64_t current_time = ::time(nullptr);
        if (current_time <
            calculate_index_expired_time(instance_id_, index_pb, &earlest_ts)) { // not expired
            return 0;
        }
        // decode index_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "index" ${index_id} -> RecycleIndexPB
        auto index_id = std::get<int64_t>(std::get<0>(out[3]));
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        std::string val;
        err = txn->get(k, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return 0;
        }
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        index_pb.Clear();
        if (!index_pb.ParseFromString(val)) {
            return 0;
        }
        if (scan_tablets_and_statistics(index_pb.table_id(), index_id, metrics_context) != 0) {
            return 0;
        }
        metrics_context.total_need_recycle_num++;
        return 0;
    };

    return scan_and_recycle(index_key0, index_key1, std::move(handle_index_kv),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                segment_metrics_context_.report(true);
                                tablet_metrics_context_.report(true);
                                return 0;
                            });
}

// Scan and statistics partitions that need to be recycled
int InstanceRecycler::scan_and_statistics_partitions() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_partitions");

    RecyclePartKeyInfo part_key_info0 {instance_id_, 0};
    RecyclePartKeyInfo part_key_info1 {instance_id_, INT64_MAX};
    std::string part_key0;
    std::string part_key1;
    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    recycle_partition_key(part_key_info0, &part_key0);
    recycle_partition_key(part_key_info1, &part_key1);
    auto handle_partition_kv = [&, this](std::string_view k, std::string_view v) -> int {
        RecyclePartitionPB part_pb;
        if (!part_pb.ParseFromArray(v.data(), v.size())) {
            return 0;
        }
        int64_t current_time = ::time(nullptr);
        if (current_time <
            calculate_partition_expired_time(instance_id_, part_pb, &earlest_ts)) { // not expired
            return 0;
        }
        // decode partition_id
        auto k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        // 0x01 "recycle" ${instance_id} "partition" ${partition_id} -> RecyclePartitionPB
        auto partition_id = std::get<int64_t>(std::get<0>(out[3]));
        // Change state to RECYCLING
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        std::string val;
        err = txn->get(k, &val);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return 0;
        }
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        part_pb.Clear();
        if (!part_pb.ParseFromString(val)) {
            return 0;
        }
        // Partitions with PREPARED state MUST have no data
        bool is_empty_tablet = part_pb.state() == RecyclePartitionPB::PREPARED;
        int ret = 0;
        for (int64_t index_id : part_pb.index_id()) {
            if (scan_tablets_and_statistics(part_pb.table_id(), index_id, metrics_context,
                                            partition_id, is_empty_tablet) != 0) {
                ret = 0;
            }
        }
        metrics_context.total_need_recycle_num++;
        return ret;
    };
    return scan_and_recycle(part_key0, part_key1, std::move(handle_partition_kv),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                segment_metrics_context_.report(true);
                                tablet_metrics_context_.report(true);
                                return 0;
                            });
}

// Scan and statistics rowsets that need to be recycled
int InstanceRecycler::scan_and_statistics_rowsets() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_rowsets");
    RecycleRowsetKeyInfo recyc_rs_key_info0 {instance_id_, 0, ""};
    RecycleRowsetKeyInfo recyc_rs_key_info1 {instance_id_, INT64_MAX, ""};
    std::string recyc_rs_key0;
    std::string recyc_rs_key1;
    recycle_rowset_key(recyc_rs_key_info0, &recyc_rs_key0);
    recycle_rowset_key(recyc_rs_key_info1, &recyc_rs_key1);
    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    auto handle_rowset_kv = [&, this](std::string_view k, std::string_view v) -> int {
        RecycleRowsetPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            return 0;
        }
        int64_t current_time = ::time(nullptr);
        if (current_time <
            calculate_rowset_expired_time(instance_id_, rowset, &earlest_ts)) { // not expired
            return 0;
        }
        if (!rowset.has_type()) {
            if (!rowset.has_resource_id()) [[unlikely]] {
                return 0;
            }
            if (rowset.resource_id().empty()) [[unlikely]] {
                return 0;
            }
            return 0;
        }
        auto* rowset_meta = rowset.mutable_rowset_meta();
        if (!rowset_meta->has_resource_id()) [[unlikely]] {
            if (rowset.type() == RecycleRowsetPB::PREPARE || rowset_meta->num_segments() != 0) {
                return 0;
            }
        }
        metrics_context.total_need_recycle_num++;
        metrics_context.total_need_recycle_data_size += rowset_meta->total_disk_size();
        segment_metrics_context_.total_need_recycle_num += rowset_meta->num_segments();
        segment_metrics_context_.total_need_recycle_data_size += rowset_meta->total_disk_size();
        return 0;
    };
    return scan_and_recycle(recyc_rs_key0, recyc_rs_key1, std::move(handle_rowset_kv),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                segment_metrics_context_.report(true);
                                return 0;
                            });
}

// Scan and statistics tmp_rowsets that need to be recycled
int InstanceRecycler::scan_and_statistics_tmp_rowsets() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_tmp_rowsets");
    MetaRowsetTmpKeyInfo tmp_rs_key_info0 {instance_id_, 0, 0};
    MetaRowsetTmpKeyInfo tmp_rs_key_info1 {instance_id_, INT64_MAX, 0};
    std::string tmp_rs_key0;
    std::string tmp_rs_key1;
    meta_rowset_tmp_key(tmp_rs_key_info0, &tmp_rs_key0);
    meta_rowset_tmp_key(tmp_rs_key_info1, &tmp_rs_key1);

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    auto handle_tmp_rowsets_kv = [&, this](std::string_view k, std::string_view v) -> int {
        doris::RowsetMetaCloudPB rowset;
        if (!rowset.ParseFromArray(v.data(), v.size())) {
            return 0;
        }
        int64_t expiration = calculate_tmp_rowset_expired_time(instance_id_, rowset, &earlest_ts);
        int64_t current_time = ::time(nullptr);
        if (current_time < expiration) {
            return 0;
        }

        DCHECK_GT(rowset.txn_id(), 0)
                << "txn_id=" << rowset.txn_id() << " rowset=" << rowset.ShortDebugString();
        if (!is_txn_finished(txn_kv_, instance_id_, rowset.txn_id())) {
            return 0;
        }

        if (!rowset.has_resource_id()) {
            if (rowset.num_segments() > 0) [[unlikely]] { // impossible
                return 0;
            }
            return 0;
        }

        metrics_context.total_need_recycle_num++;
        metrics_context.total_need_recycle_data_size += rowset.total_disk_size();
        segment_metrics_context_.total_need_recycle_data_size += rowset.total_disk_size();
        segment_metrics_context_.total_need_recycle_num += rowset.num_segments();
        return 0;
    };
    return scan_and_recycle(tmp_rs_key0, tmp_rs_key1, std::move(handle_tmp_rowsets_kv),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                segment_metrics_context_.report(true);
                                return 0;
                            });
}

// Scan and statistics abort_timeout_txn that need to be recycled
int InstanceRecycler::scan_and_statistics_abort_timeout_txn() {
    RecyclerMetricsContext metrics_context(instance_id_, "abort_timeout_txn");

    TxnRunningKeyInfo txn_running_key_info0 {instance_id_, 0, 0};
    TxnRunningKeyInfo txn_running_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_txn_running_key;
    std::string end_txn_running_key;
    txn_running_key(txn_running_key_info0, &begin_txn_running_key);
    txn_running_key(txn_running_key_info1, &end_txn_running_key);

    int64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto handle_abort_timeout_txn_kv = [&metrics_context, &current_time, this](
                                               std::string_view k, std::string_view v) -> int {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        std::string_view k1 = k;
        k1.remove_prefix(1);
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        if (decode_key(&k1, &out) != 0) {
            return 0;
        }
        int64_t db_id = std::get<int64_t>(std::get<0>(out[3]));
        int64_t txn_id = std::get<int64_t>(std::get<0>(out[4]));
        // Update txn_info
        std::string txn_inf_key, txn_inf_val;
        txn_info_key({instance_id_, db_id, txn_id}, &txn_inf_key);
        err = txn->get(txn_inf_key, &txn_inf_val);
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        TxnInfoPB txn_info;
        if (!txn_info.ParseFromString(txn_inf_val)) {
            return 0;
        }

        if (TxnStatusPB::TXN_STATUS_COMMITTED != txn_info.status()) {
            TxnRunningPB txn_running_pb;
            if (!txn_running_pb.ParseFromArray(v.data(), v.size())) {
                return 0;
            }
            if (!config::force_immediate_recycle && txn_running_pb.timeout_time() > current_time) {
                return 0;
            }
            metrics_context.total_need_recycle_num++;
        }
        return 0;
    };
    return scan_and_recycle(begin_txn_running_key, end_txn_running_key,
                            std::move(handle_abort_timeout_txn_kv), [&metrics_context]() -> int {
                                metrics_context.report(true);
                                return 0;
                            });
}

// Scan and statistics expired_txn_label that need to be recycled
int InstanceRecycler::scan_and_statistics_expired_txn_label() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_expired_txn_label");

    RecycleTxnKeyInfo recycle_txn_key_info0 {instance_id_, 0, 0};
    RecycleTxnKeyInfo recycle_txn_key_info1 {instance_id_, INT64_MAX, INT64_MAX};
    std::string begin_recycle_txn_key;
    std::string end_recycle_txn_key;
    recycle_txn_key(recycle_txn_key_info0, &begin_recycle_txn_key);
    recycle_txn_key(recycle_txn_key_info1, &end_recycle_txn_key);
    int64_t earlest_ts = std::numeric_limits<int64_t>::max();
    int64_t current_time_ms =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    // for calculate the total num or bytes of recyled objects
    auto handle_expired_txn_label_kv = [&, this](std::string_view k, std::string_view v) -> int {
        RecycleTxnPB recycle_txn_pb;
        if (!recycle_txn_pb.ParseFromArray(v.data(), v.size())) {
            return 0;
        }
        if ((config::force_immediate_recycle) ||
            (recycle_txn_pb.has_immediate() && recycle_txn_pb.immediate()) ||
            (calculate_txn_expired_time(instance_id_, recycle_txn_pb, &earlest_ts) <=
             current_time_ms)) {
            metrics_context.total_need_recycle_num++;
        }
        return 0;
    };
    return scan_and_recycle(begin_recycle_txn_key, end_recycle_txn_key,
                            std::move(handle_expired_txn_label_kv), [&metrics_context]() -> int {
                                metrics_context.report(true);
                                return 0;
                            });
}

// Scan and statistics copy_jobs that need to be recycled
int InstanceRecycler::scan_and_statistics_copy_jobs() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_copy_jobs");
    CopyJobKeyInfo key_info0 {instance_id_, "", 0, "", 0};
    CopyJobKeyInfo key_info1 {instance_id_, "\xff", 0, "", 0};
    std::string key0;
    std::string key1;
    copy_job_key(key_info0, &key0);
    copy_job_key(key_info1, &key1);

    // for calculate the total num or bytes of recyled objects
    auto scan_and_statistics = [&metrics_context](std::string_view k, std::string_view v) -> int {
        CopyJobPB copy_job;
        if (!copy_job.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed copy job").tag("key", hex(k));
            return 0;
        }

        if (copy_job.job_status() == CopyJobPB::FINISH) {
            if (copy_job.stage_type() == StagePB::EXTERNAL) {
                int64_t current_time =
                        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                if (copy_job.finish_time_ms() > 0) {
                    if (!config::force_immediate_recycle &&
                        current_time < copy_job.finish_time_ms() +
                                               config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                } else {
                    if (!config::force_immediate_recycle &&
                        current_time < copy_job.start_time_ms() +
                                               config::copy_job_max_retention_second * 1000) {
                        return 0;
                    }
                }
            }
        } else if (copy_job.job_status() == CopyJobPB::LOADING) {
            int64_t current_time =
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            if (!config::force_immediate_recycle && current_time <= copy_job.timeout_time_ms()) {
                return 0;
            }
        }
        metrics_context.total_need_recycle_num++;
        return 0;
    };

    return scan_and_recycle(key0, key1, std::move(scan_and_statistics),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                return 0;
                            });
}

// Scan and statistics stage that need to be recycled
int InstanceRecycler::scan_and_statistics_stage() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_stage");
    RecycleStageKeyInfo key_info0 {instance_id_, ""};
    RecycleStageKeyInfo key_info1 {instance_id_, "\xff"};
    std::string key0 = recycle_stage_key(key_info0);
    std::string key1 = recycle_stage_key(key_info1);

    // for calculate the total num or bytes of recyled objects
    auto scan_and_statistics = [&metrics_context, this](std::string_view k,
                                                        std::string_view v) -> int {
        RecycleStagePB recycle_stage;
        if (!recycle_stage.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle stage").tag("key", hex(k));
            return 0;
        }

        int idx = stoi(recycle_stage.stage().obj_info().id());
        if (idx > instance_info_.obj_info().size() || idx < 1) {
            LOG(WARNING) << "invalid idx: " << idx;
            return 0;
        }

        std::shared_ptr<StorageVaultAccessor> accessor;
        int ret = SYNC_POINT_HOOK_RETURN_VALUE(
                [&] {
                    auto& old_obj = instance_info_.obj_info()[idx - 1];
                    auto s3_conf = S3Conf::from_obj_store_info(old_obj);
                    if (!s3_conf) {
                        return 0;
                    }

                    s3_conf->prefix = recycle_stage.stage().obj_info().prefix();
                    std::shared_ptr<S3Accessor> s3_accessor;
                    int ret = S3Accessor::create(std::move(s3_conf.value()), &s3_accessor);
                    if (ret != 0) {
                        return 0;
                    }

                    accessor = std::move(s3_accessor);
                    return 0;
                }(),
                "recycle_stage:get_accessor", &accessor);

        if (ret != 0) {
            LOG(WARNING) << "failed to init accessor ret=" << ret;
            return 0;
        }

        metrics_context.total_need_recycle_num++;
        return 0;
    };

    return scan_and_recycle(key0, key1, std::move(scan_and_statistics),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                return 0;
                            });
}

// Scan and statistics expired_stage_objects that need to be recycled
int InstanceRecycler::scan_and_statistics_expired_stage_objects() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_expired_stage_objects");

    // for calculate the total num or bytes of recyled objects
    auto scan_and_statistics = [&metrics_context, this]() {
        for (const auto& stage : instance_info_.stages()) {
            if (stopped()) {
                break;
            }
            if (stage.type() == StagePB::EXTERNAL) {
                continue;
            }
            int idx = stoi(stage.obj_info().id());
            if (idx > instance_info_.obj_info().size() || idx < 1) {
                continue;
            }
            const auto& old_obj = instance_info_.obj_info()[idx - 1];
            auto s3_conf = S3Conf::from_obj_store_info(old_obj);
            if (!s3_conf) {
                continue;
            }
            s3_conf->prefix = stage.obj_info().prefix();
            std::shared_ptr<S3Accessor> accessor;
            int ret1 = S3Accessor::create(*s3_conf, &accessor);
            if (ret1 != 0) {
                continue;
            }
            if (s3_conf->prefix.find("/stage/") == std::string::npos) {
                continue;
            }
            metrics_context.total_need_recycle_num++;
        }
    };

    scan_and_statistics();
    metrics_context.report(true);
    return 0;
}

// Scan and statistics versions that need to be recycled
int InstanceRecycler::scan_and_statistics_versions() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_versions");
    auto version_key_begin = partition_version_key({instance_id_, 0, 0, 0});
    auto version_key_end = partition_version_key({instance_id_, INT64_MAX, 0, 0});

    int64_t last_scanned_table_id = 0;
    bool is_recycled = false; // Is last scanned kv recycled
    // for calculate the total num or bytes of recyled objects
    auto scan_and_statistics = [&metrics_context, &last_scanned_table_id, &is_recycled, this](
                                       std::string_view k, std::string_view) {
        auto k1 = k;
        k1.remove_prefix(1);
        // 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id}
        std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
        decode_key(&k1, &out);
        DCHECK_EQ(out.size(), 6) << k;
        auto table_id = std::get<int64_t>(std::get<0>(out[4]));
        if (table_id == last_scanned_table_id) { // Already handle kvs of this table
            metrics_context.total_need_recycle_num +=
                    is_recycled; // Version kv of this table has been recycled
            return 0;
        }
        last_scanned_table_id = table_id;
        is_recycled = false;
        auto tablet_key_begin = stats_tablet_key({instance_id_, table_id, 0, 0, 0});
        auto tablet_key_end = stats_tablet_key({instance_id_, table_id, INT64_MAX, 0, 0});
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        std::unique_ptr<RangeGetIterator> iter;
        err = txn->get(tablet_key_begin, tablet_key_end, &iter, false, 1);
        if (err != TxnErrorCode::TXN_OK) {
            return 0;
        }
        if (iter->has_next()) { // Table is useful, should not recycle table and partition versions
            return 0;
        }
        metrics_context.total_need_recycle_num++;
        is_recycled = true;
        return 0;
    };

    return scan_and_recycle(version_key_begin, version_key_end, std::move(scan_and_statistics),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                return 0;
                            });
}

// Scan and statistics restore jobs that need to be recycled
int InstanceRecycler::scan_and_statistics_restore_jobs() {
    RecyclerMetricsContext metrics_context(instance_id_, "recycle_restore_jobs");
    JobRestoreTabletKeyInfo restore_job_key_info0 {instance_id_, 0};
    JobRestoreTabletKeyInfo restore_job_key_info1 {instance_id_, INT64_MAX};
    std::string restore_job_key0;
    std::string restore_job_key1;
    job_restore_tablet_key(restore_job_key_info0, &restore_job_key0);
    job_restore_tablet_key(restore_job_key_info1, &restore_job_key1);

    int64_t earlest_ts = std::numeric_limits<int64_t>::max();

    // for calculate the total num or bytes of recyled objects
    auto scan_and_statistics = [&](std::string_view k, std::string_view v) -> int {
        RestoreJobCloudPB restore_job_pb;
        if (!restore_job_pb.ParseFromArray(v.data(), v.size())) {
            LOG_WARNING("malformed recycle partition value").tag("key", hex(k));
            return 0;
        }
        int64_t expiration =
                calculate_restore_job_expired_time(instance_id_, restore_job_pb, &earlest_ts);
        int64_t current_time = ::time(nullptr);
        if (current_time < expiration) { // not expired
            return 0;
        }
        metrics_context.total_need_recycle_num++;
        return 0;
    };

    return scan_and_recycle(restore_job_key0, restore_job_key1, std::move(scan_and_statistics),
                            [&metrics_context]() -> int {
                                metrics_context.report(true);
                                return 0;
                            });
}

} // namespace doris::cloud
