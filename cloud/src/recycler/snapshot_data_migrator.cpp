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

#include "recycler/snapshot_data_migrator.h"

#include <gen_cpp/cloud.pb.h>

#include "common/config.h"
#include "common/defer.h"
#include "common/stopwatch.h"
#include "common/util.h"
#include "meta-store/keys.h"
#include "mock_accessor.h"
#include "recycler/hdfs_accessor.h"
#include "recycler/s3_accessor.h"
#include "recycler/util.h"

namespace doris::cloud {

SnapshotDataMigrator::SnapshotDataMigrator(std::shared_ptr<TxnKv> txn_kv)
        : txn_kv_(std::move(txn_kv)) {}

SnapshotDataMigrator::~SnapshotDataMigrator() {
    if (!stopped()) {
        stop();
    }
}

int SnapshotDataMigrator::start() {
    workers_.emplace_back([this]() { scan_instance_loop(); });
    workers_.emplace_back([this] { lease_migration_jobs(); });
    for (int i = 0; i < config::snapshot_data_migrator_concurrent; ++i) {
        workers_.emplace_back([this]() { migration_loop(); });
    }

    LOG_INFO("snapshot data migrator started")
            .tag("concurrent", config::snapshot_data_migrator_concurrent);

    return 0;
}

void SnapshotDataMigrator::stop() {
    stopped_ = true;
    notifier_.notify_all();
    pending_instance_cond_.notify_all();
    {
        std::lock_guard lock(mtx_);
        for (auto& [_, migrator] : migrating_instance_map_) {
            migrator->stop();
        }
    }
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

void SnapshotDataMigrator::migration_loop() {
    pthread_setname_np(pthread_self(), "SNAP_MIGRATOR");
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
            // skip instance in recycling
            if (migrating_instance_map_.count(instance_id)) continue;
        }

        auto migrator = std::make_shared<InstanceDataMigrator>(txn_kv_, instance);
        if (migrator->init() != 0) {
            LOG(WARNING) << "failed to init instance migrator, instance_id="
                         << instance.instance_id();
            continue;
        }

        std::string job_key = job_snapshot_data_migrator_key(instance.instance_id());
        int ret =
                prepare_instance_recycle_job(txn_kv_.get(), job_key, instance.instance_id(),
                                             ip_port_, config::recycle_job_lease_expired_ms * 1000);
        if (ret != 0) { // Prepare failed
            continue;
        } else {
            std::lock_guard lock(mtx_);
            migrating_instance_map_.emplace(instance_id, migrator);
        }
        if (stopped()) return;

        migrator->do_migrate();
        {
            std::lock_guard lock(mtx_);
            migrating_instance_map_.erase(instance.instance_id());
        }
    }
}

void SnapshotDataMigrator::scan_instance_loop() {
    std::this_thread::sleep_for(
            std::chrono::seconds(config::recycler_sleep_before_scheduling_seconds));
    while (!stopped()) {
        std::vector<InstanceInfoPB> instances;
        get_all_instances(txn_kv_.get(), instances);
        LOG(INFO) << "Snapshot data migrator get instances: " << [&instances] {
            std::stringstream ss;
            for (auto& i : instances) ss << ' ' << i.instance_id();
            return ss.str();
        }();
        if (!instances.empty()) {
            // enqueue instances
            std::lock_guard lock(mtx_);
            for (auto& instance : instances) {
                if (instance.status() == InstanceInfoPB::DELETED) continue;
                if (!is_instance_need_migrate(instance)) continue;
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
            notifier_.wait_for(lock, std::chrono::seconds(config::scan_instances_interval_seconds),
                               [&]() { return stopped(); });
        }
    }
}

void SnapshotDataMigrator::lease_migration_jobs() {
    while (!stopped()) {
        std::vector<std::string> instances;
        instances.reserve(migrating_instance_map_.size());
        {
            std::lock_guard lock(mtx_);
            for (auto& [id, _] : migrating_instance_map_) {
                instances.push_back(id);
            }
        }
        for (auto& i : instances) {
            std::string job_key = job_snapshot_data_migrator_key(i);
            int ret = lease_instance_recycle_job(txn_kv_.get(), job_key, i, ip_port_);
            if (ret == 1) {
                std::lock_guard lock(mtx_);
                if (auto it = migrating_instance_map_.find(i);
                    it != migrating_instance_map_.end()) {
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

bool SnapshotDataMigrator::is_instance_need_migrate(const InstanceInfoPB& instance_info) {
    // The multi-version feature is enabled, but the snapshot feature is not ready.
    return instance_info.multi_version_status() != MultiVersionStatus::MULTI_VERSION_DISABLED &&
           instance_info.snapshot_switch_status() == SnapshotSwitchStatus::SNAPSHOT_SWITCH_DISABLED;
}

InstanceDataMigrator::InstanceDataMigrator(std::shared_ptr<TxnKv> txn_kv,
                                           const InstanceInfoPB& instance)
        : txn_kv_(std::move(txn_kv)),
          instance_id_(instance.instance_id()),
          instance_info_(instance) {}

InstanceDataMigrator::~InstanceDataMigrator() {
    if (!stopped()) {
        stop();
    }
}

int InstanceDataMigrator::init() {
    int ret = init_obj_store_accessors();
    if (ret != 0) {
        return ret;
    }

    return init_storage_vault_accessors();
}

int InstanceDataMigrator::init_obj_store_accessors() {
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

int InstanceDataMigrator::init_storage_vault_accessors() {
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

int InstanceDataMigrator::do_migrate() {
    LOG_WARNING("start data migration").tag("instance_id", instance_id_);

    StopWatch stop_watch;
    DORIS_CLOUD_DEFER {
        LOG_WARNING("data migration done")
                .tag("instance_id", instance_id_)
                .tag("cost(sec)", stop_watch.elapsed_seconds());
    };

    SnapshotManager snapshot_mgr(txn_kv_);
    int res = snapshot_mgr.migrate_to_versioned_keys(this);
    if (res != 0) {
        LOG_WARNING("failed to migrate snapshot keys").tag("instance_id", instance_id_);
        return res;
    }

    return enable_instance_snapshot_switch();
}

int InstanceDataMigrator::enable_instance_snapshot_switch() {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn for data migration").tag("instance_id", instance_id_);
        return -1;
    }

    std::string key = instance_key(instance_id_);
    std::string instance_value;
    err = txn->get(key, &instance_value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to get instance info in data migration")
                .tag("instance_id", instance_id_)
                .tag("error", err);
        return -1;
    }

    InstanceInfoPB instance_info;
    if (!instance_info.ParseFromString(instance_value)) {
        LOG_WARNING("failed to parse instance info in data migration")
                .tag("instance_id", instance_id_);
        return -1;
    }

    if (instance_info.multi_version_status() == MultiVersionStatus::MULTI_VERSION_DISABLED) {
        LOG_WARNING("instance multi version status is disabled, no need to enable snapshot switch")
                .tag("instance_id", instance_id_);
        return 0;
    }

    instance_info.set_snapshot_switch_status(SnapshotSwitchStatus::SNAPSHOT_SWITCH_OFF);
    instance_info.clear_migrated_key_sets();
    txn->atomic_add(system_meta_service_instance_update_key(), 1);
    txn->put(key, instance_info.SerializeAsString());
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to commit instance info in data migration")
                .tag("instance_id", instance_id_)
                .tag("error", err);
        return -1;
    }

    LOG_WARNING("finish data migration").tag("instance_id", instance_id_);

    return 0;
}

} // namespace doris::cloud
