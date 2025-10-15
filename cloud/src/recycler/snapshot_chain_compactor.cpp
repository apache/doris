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

#include "recycler/snapshot_chain_compactor.h"

#include <gen_cpp/cloud.pb.h>

#include "common/config.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-store/keys.h"
#include "mock_accessor.h"
#include "recycler/hdfs_accessor.h"
#include "recycler/s3_accessor.h"
#include "recycler/util.h"

namespace doris::cloud {

SnapshotChainCompactor::SnapshotChainCompactor(std::shared_ptr<TxnKv> txn_kv)
        : txn_kv_(std::move(txn_kv)) {}

SnapshotChainCompactor::~SnapshotChainCompactor() {
    if (!stopped()) {
        stop();
    }
}

int SnapshotChainCompactor::start() {
    workers_.emplace_back([this]() { scan_instance_loop(); });
    workers_.emplace_back([this] { lease_compaction_jobs(); });
    for (int i = 0; i < config::snapshot_chain_compactor_concurrent; ++i) {
        workers_.emplace_back([this]() { compaction_loop(); });
    }

    LOG_INFO("snapshot chain compactor started")
            .tag("concurrent", config::snapshot_chain_compactor_concurrent);

    return 0;
}

void SnapshotChainCompactor::stop() {
    stopped_ = true;
    notifier_.notify_all();
    pending_instance_cond_.notify_all();
    {
        std::lock_guard lock(mtx_);
        for (auto& [_, compactor] : compacting_instance_map_) {
            compactor->stop();
        }
    }
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

void SnapshotChainCompactor::compaction_loop() {
    while (!stopped()) {
        // fetch instance to check
        InstanceInfoPB instance;
        {
            std::unique_lock lock(mtx_);
            pending_instance_cond_.wait(
                    lock, [&]() -> bool { return !pending_instance_queue_.empty() || stopped(); });
            if (stopped()) {
                return;
            }
            instance = std::move(pending_instance_queue_.front());
            pending_instance_queue_.pop_front();
            pending_instance_set_.erase(instance.instance_id());
        }
        const auto& instance_id = instance.instance_id();
        {
            std::lock_guard lock(mtx_);
            // skip instance in compacting
            if (compacting_instance_map_.count(instance_id)) continue;
        }

        auto compactor = std::make_shared<InstanceChainCompactor>(txn_kv_, instance);
        if (compactor->init() != 0) {
            LOG(WARNING) << "failed to init instance compactor, instance_id="
                         << instance.instance_id();
            continue;
        }

        std::string job_key = job_snapshot_chain_compactor_key(instance.instance_id());
        int ret =
                prepare_instance_recycle_job(txn_kv_.get(), job_key, instance.instance_id(),
                                             ip_port_, config::recycle_job_lease_expired_ms * 1000);
        if (ret != 0) { // Prepare failed
            continue;
        } else {
            std::lock_guard lock(mtx_);
            compacting_instance_map_.emplace(instance_id, compactor);
        }
        if (stopped()) return;

        compactor->do_compact();
        {
            std::lock_guard lock(mtx_);
            compacting_instance_map_.erase(instance.instance_id());
        }
    }
}

void SnapshotChainCompactor::scan_instance_loop() {
    std::this_thread::sleep_for(
            std::chrono::seconds(config::recycler_sleep_before_scheduling_seconds));
    while (!stopped()) {
        std::vector<InstanceInfoPB> instances;
        get_all_instances(txn_kv_.get(), instances);
        LOG(INFO) << "Snapshot chain compactor get instances: " << [&instances] {
            std::stringstream ss;
            for (auto& i : instances) ss << ' ' << i.instance_id();
            return ss.str();
        }();
        if (!instances.empty()) {
            // enqueue instances
            std::lock_guard lock(mtx_);
            for (auto& instance : instances) {
                if (instance.status() == InstanceInfoPB::DELETED) continue;
                if (!is_snapshot_chain_need_compact(instance)) continue;
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

void SnapshotChainCompactor::lease_compaction_jobs() {
    while (!stopped()) {
        std::vector<std::string> instances;
        instances.reserve(compacting_instance_map_.size());
        {
            std::lock_guard lock(mtx_);
            for (auto& [id, _] : compacting_instance_map_) {
                instances.push_back(id);
            }
        }
        for (auto& i : instances) {
            std::string job_key = job_snapshot_chain_compactor_key(i);
            int ret = lease_instance_recycle_job(txn_kv_.get(), job_key, i, ip_port_);
            if (ret == 1) {
                std::lock_guard lock(mtx_);
                if (auto it = compacting_instance_map_.find(i);
                    it != compacting_instance_map_.end()) {
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

bool SnapshotChainCompactor::is_snapshot_chain_need_compact(const InstanceInfoPB& instance_info) {
    // TODO:
    return false;
}

InstanceChainCompactor::InstanceChainCompactor(std::shared_ptr<TxnKv> txn_kv,
                                               const InstanceInfoPB& instance)
        : txn_kv_(std::move(txn_kv)),
          instance_id_(instance.instance_id()),
          instance_info_(instance) {}

InstanceChainCompactor::~InstanceChainCompactor() {
    if (!stopped()) {
        stop();
    }
}

int InstanceChainCompactor::init() {
    int ret = init_obj_store_accessors();
    if (ret != 0) {
        return ret;
    }

    return init_storage_vault_accessors();
}

int InstanceChainCompactor::init_obj_store_accessors() {
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
            LOG(WARNING) << "failed to init object accessor. instance_id=" << instance_id_
                         << " resource_id=" << obj_info.id();
            return ret;
        }
#endif

        accessor_map_.emplace(obj_info.id(), std::move(accessor));
    }

    return 0;
}

int InstanceChainCompactor::init_storage_vault_accessors() {
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
        TEST_SYNC_POINT_CALLBACK("InstanceRecycler::init_storage_vault_accessors.mock_vault",
                                 &accessor_map_, &vault);
        if (vault.has_hdfs_info()) {
#ifdef ENABLE_HDFS_STORAGE_VAULT
            auto accessor = std::make_shared<HdfsAccessor>(vault.hdfs_info());
            int ret = accessor->init();
            if (ret != 0) {
                LOG(WARNING) << "failed to init hdfs accessor. instance_id=" << instance_id_
                             << " resource_id=" << vault.id() << " name=" << vault.name();
                return ret;
            }

            accessor_map_.emplace(vault.id(), std::move(accessor));
#else
            LOG(ERROR) << "HDFS is disabled (via the ENABLE_HDFS_STORAGE_VAULT build option), "
                       << "but HDFS storage vaults were detected";
#endif
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

int InstanceChainCompactor::do_compact() {
    LOG_WARNING("start snapshot chain compaction").tag("instance_id", instance_id_);

    StopWatch stop_watch;
    DORIS_CLOUD_DEFER {
        LOG_WARNING("snapshot chain compaction done")
                .tag("instance_id", instance_id_)
                .tag("cost(sec)", stop_watch.elapsed_seconds());
    };

    SnapshotManager snapshot_mgr(txn_kv_);
    int res = snapshot_mgr.compact_snapshot_chains(this);
    if (res != 0) {
        LOG_WARNING("failed to compact snapshot chains").tag("instance_id", instance_id_);
        return res;
    }

    return handle_compaction_completion();
}

int InstanceChainCompactor::handle_compaction_completion() {
    // TODO:
    return 0;
}

} // namespace doris::cloud