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

#include "snapshot/doris_snapshot_manager.h"

#include <arpa/inet.h>
#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>

#include <chrono>
#include <unordered_set>

#include "common/config.h"
#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/checker.h"
#include "recycler/meta_checker.h"
#include "recycler/recycler.h"
#include "recycler/snapshot_chain_compactor.h"
#include "recycler/snapshot_data_migrator.h"
#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {

static int64_t current_time_seconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

// ============================================================================
// Constructor
// ============================================================================

DorisSnapshotManager::DorisSnapshotManager(std::shared_ptr<TxnKv> txn_kv)
        : SnapshotManager(std::move(txn_kv)) {}

// ============================================================================
// Internal helpers
// ============================================================================

TxnErrorCode DorisSnapshotManager::read_snapshot_pb(Transaction* txn, std::string_view instance_id,
                                                    const Versionstamp& vs, SnapshotPB* pb) {
    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string full_key = encode_versioned_key(key_prefix, vs);

    std::string value;
    TxnErrorCode err = txn->get(full_key, &value);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (!pb->ParseFromString(value)) {
        LOG(WARNING) << "Failed to parse SnapshotPB, instance_id=" << instance_id
                     << " snapshot_id=" << vs.to_string() << " key=" << hex(full_key);
        return TxnErrorCode::TXN_INVALID_DATA;
    }
    return TxnErrorCode::TXN_OK;
}

void DorisSnapshotManager::write_snapshot_pb(Transaction* txn, std::string_view instance_id,
                                             const Versionstamp& vs, const SnapshotPB& pb) {
    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string full_key = encode_versioned_key(key_prefix, vs);
    std::string value = pb.SerializeAsString();
    txn->put(full_key, value);
}

TxnErrorCode DorisSnapshotManager::read_instance_info(Transaction* txn,
                                                      std::string_view instance_id,
                                                      InstanceInfoPB* info) {
    std::string key = instance_key({std::string(instance_id)});
    std::string value;
    TxnErrorCode err = txn->get(key, &value);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }

    if (!info->ParseFromString(value)) {
        LOG(WARNING) << "Failed to parse InstanceInfoPB, instance_id=" << instance_id;
        return TxnErrorCode::TXN_INVALID_DATA;
    }
    return TxnErrorCode::TXN_OK;
}

bool DorisSnapshotManager::validate_ip_address(const std::string& ip) {
    if (ip.empty()) return true; // IP is optional
    struct in_addr ipv4_addr;
    struct in6_addr ipv6_addr;
    return inet_pton(AF_INET, ip.c_str(), &ipv4_addr) == 1 ||
           inet_pton(AF_INET6, ip.c_str(), &ipv6_addr) == 1;
}

std::string DorisSnapshotManager::build_image_url(const InstanceInfoPB& instance,
                                                  const std::string& snapshot_id) {
    // Use the first obj_info's prefix as the storage root.
    std::string prefix;
    if (instance.obj_info_size() > 0 && !instance.obj_info(0).prefix().empty()) {
        prefix = instance.obj_info(0).prefix();
        if (prefix.back() != '/') {
            prefix += '/';
        }
    }
    return prefix + "snapshot/" + snapshot_id + "/image/";
}

void DorisSnapshotManager::snapshot_pb_to_info(const SnapshotPB& pb, const Versionstamp& vs,
                                               SnapshotInfoPB* info) {
    info->set_snapshot_id(serialize_snapshot_id(vs));
    if (pb.has_snapshot_ancestor()) {
        info->set_ancestor_id(pb.snapshot_ancestor());
    }
    if (pb.has_create_at()) info->set_create_at(pb.create_at());
    if (pb.has_finish_at()) info->set_finish_at(pb.finish_at());
    if (pb.has_image_url()) info->set_image_url(pb.image_url());
    if (pb.has_last_journal_id()) info->set_journal_id(pb.last_journal_id());
    info->set_status(pb.status());
    info->set_type(pb.type());
    if (pb.has_instance_id()) info->set_instance_id(pb.instance_id());
    if (pb.has_auto_()) info->set_auto_snapshot(pb.auto_());
    if (pb.has_ttl_seconds()) info->set_ttl_seconds(pb.ttl_seconds());
    if (pb.has_timeout_seconds()) info->set_timeout_seconds(pb.timeout_seconds());
    if (pb.has_label()) info->set_snapshot_label(pb.label());
    if (pb.has_reason()) info->set_reason(pb.reason());
    if (pb.has_snapshot_meta_image_size()) {
        info->set_snapshot_meta_image_size(pb.snapshot_meta_image_size());
    }
    if (pb.has_snapshot_logical_data_size()) {
        info->set_snapshot_logical_data_size(pb.snapshot_logical_data_size());
    }
    if (pb.has_snapshot_retained_data_size()) {
        info->set_snapshot_retained_data_size(pb.snapshot_retained_data_size());
    }
    if (pb.has_snapshot_billable_data_size()) {
        info->set_snapshot_billable_data_size(pb.snapshot_billable_data_size());
    }
}

/// Abort an orphan PREPARE snapshot created during begin_snapshot
/// when the second transaction (image_url update) fails.
void DorisSnapshotManager::abort_orphan_snapshot(std::string_view instance_id,
                                                 const Versionstamp& vs) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "abort_orphan_snapshot: cannot create txn"
                     << ", instance_id=" << instance_id;
        return;
    }
    SnapshotPB pb;
    err = read_snapshot_pb(txn.get(), instance_id, vs, &pb);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "abort_orphan_snapshot: cannot read pb"
                     << ", instance_id=" << instance_id;
        return;
    }
    pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    pb.set_reason("orphan: begin_snapshot txn2 failed");
    write_snapshot_pb(txn.get(), instance_id, vs, pb);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "abort_orphan_snapshot: commit failed"
                     << ", instance_id=" << instance_id;
    } else {
        LOG(INFO) << "abort_orphan_snapshot: aborted orphan"
                  << ", instance_id=" << instance_id << ", snapshot_id=" << vs.to_string();
    }
}

// ============================================================================
// begin_snapshot
// ============================================================================

void DorisSnapshotManager::begin_snapshot(std::string_view instance_id,
                                          const BeginSnapshotRequest& request,
                                          BeginSnapshotResponse* response) {
    // 1. Parameter validation
    if (!request.has_timeout_seconds() || request.timeout_seconds() <= 0) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("timeout_seconds must be positive");
        return;
    }
    if (!request.has_ttl_seconds() || request.ttl_seconds() <= 0) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("ttl_seconds must be positive");
        return;
    }
    if (!request.has_snapshot_label() || request.snapshot_label().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_label must not be empty");
        return;
    }
    // Optional IP validation
    if (request.has_request_ip() && !request.request_ip().empty()) {
        if (!validate_ip_address(request.request_ip())) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg("invalid request_ip format");
            return;
        }
    }

    // 2. Read instance info to get obj_info
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create txn");
        return;
    }

    InstanceInfoPB instance;
    err = read_instance_info(txn.get(), instance_id, &instance);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to read instance info");
        return;
    }

    // 3. Build SnapshotPB
    SnapshotPB snapshot_pb;
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_PREPARE);
    snapshot_pb.set_type(SnapshotType::SNAPSHOT_REFERENCE);
    snapshot_pb.set_instance_id(std::string(instance_id));
    snapshot_pb.set_create_at(current_time_seconds());
    snapshot_pb.set_timeout_seconds(request.timeout_seconds());
    snapshot_pb.set_ttl_seconds(request.ttl_seconds());
    snapshot_pb.set_label(request.snapshot_label());
    if (request.has_auto_snapshot()) {
        snapshot_pb.set_auto_(request.auto_snapshot());
    }
    // Set resource_id from first obj_info
    if (instance.obj_info_size() > 0 && instance.obj_info(0).has_id()) {
        snapshot_pb.set_resource_id(instance.obj_info(0).id());
    }

    // 4. Use versioned_put (FDB atomic versionstamp) to write SnapshotPB
    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string value = snapshot_pb.SerializeAsString();
    txn->enable_get_versionstamp();
    versioned_put(txn.get(), key_prefix, value);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
        response->mutable_status()->set_msg("failed to commit txn for begin_snapshot");
        return;
    }

    // 5. Retrieve the assigned versionstamp
    Versionstamp vs;
    err = txn->get_versionstamp(&vs);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
        response->mutable_status()->set_msg("failed to get versionstamp after commit");
        return;
    }

    // 6. Now update image_url with the actual snapshot_id (requires a second txn)
    //    because the versionstamp is only known after txn1 commits.
    std::string snapshot_id = serialize_snapshot_id(vs);
    std::string image_url = build_image_url(instance, snapshot_id);

    // Update the SnapshotPB with image_url in a new txn.
    // Re-read to detect concurrent modification (TOCTOU protection).
    {
        std::unique_ptr<Transaction> txn2;
        err = txn_kv_->create_txn(&txn2);
        if (err != TxnErrorCode::TXN_OK) {
            // Abort the orphan PREPARE snapshot
            abort_orphan_snapshot(instance_id, vs);
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
            response->mutable_status()->set_msg("failed to create txn for image_url update");
            return;
        }
        // Conflict detection: re-read and verify still PREPARE
        SnapshotPB existing_pb;
        err = read_snapshot_pb(txn2.get(), instance_id, vs, &existing_pb);
        if (err != TxnErrorCode::TXN_OK ||
            existing_pb.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
            LOG(WARNING) << "begin_snapshot: concurrent modification "
                         << "detected, instance_id=" << instance_id
                         << " snapshot_id=" << snapshot_id;
            response->mutable_status()->set_code(MetaServiceCode::UNDEFINED_ERR);
            response->mutable_status()->set_msg("concurrent modification during begin");
            return;
        }
        snapshot_pb.set_image_url(image_url);
        write_snapshot_pb(txn2.get(), instance_id, vs, snapshot_pb);
        err = txn2->commit();
        if (err != TxnErrorCode::TXN_OK) {
            // Abort orphan PREPARE snapshot on commit failure
            abort_orphan_snapshot(instance_id, vs);
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
            response->mutable_status()->set_msg("failed to commit image_url update");
            return;
        }
    }

    // 7. Build response
    response->mutable_status()->set_code(MetaServiceCode::OK);
    response->set_snapshot_id(snapshot_id);
    response->set_image_url(image_url);
    if (instance.obj_info_size() > 0) {
        response->mutable_obj_info()->CopyFrom(instance.obj_info(0));
    }

    LOG(INFO) << "begin_snapshot success, instance_id=" << instance_id
              << " snapshot_id=" << snapshot_id << " label=" << request.snapshot_label();
}

// ============================================================================
// update_snapshot
// ============================================================================

void DorisSnapshotManager::update_snapshot(std::string_view instance_id,
                                           const UpdateSnapshotRequest& request,
                                           UpdateSnapshotResponse* response) {
    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("invalid snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create txn");
        return;
    }

    SnapshotPB snapshot_pb;
    err = read_snapshot_pb(txn.get(), instance_id, vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot not found");
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to read snapshot");
        return;
    }

    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot is not in PREPARE state, current status=" +
                                            std::to_string(static_cast<int>(snapshot_pb.status())));
        return;
    }

    // Update upload tracking fields
    if (request.has_upload_file()) {
        snapshot_pb.set_upload_file(request.upload_file());
    }
    if (request.has_upload_id()) {
        snapshot_pb.set_upload_id(request.upload_id());
    }

    write_snapshot_pb(txn.get(), instance_id, vs, snapshot_pb);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
        response->mutable_status()->set_msg("failed to commit update_snapshot");
        return;
    }

    response->mutable_status()->set_code(MetaServiceCode::OK);
    LOG(INFO) << "update_snapshot success, instance_id=" << instance_id
              << " snapshot_id=" << request.snapshot_id();
}

// ============================================================================
// commit_snapshot
// ============================================================================

void DorisSnapshotManager::commit_snapshot(std::string_view instance_id,
                                           const CommitSnapshotRequest& request,
                                           CommitSnapshotResponse* response) {
    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("invalid snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create txn");
        return;
    }

    SnapshotPB snapshot_pb;
    err = read_snapshot_pb(txn.get(), instance_id, vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot not found");
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to read snapshot");
        return;
    }

    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot is not in PREPARE state, current status=" +
                                            std::to_string(static_cast<int>(snapshot_pb.status())));
        return;
    }

    // Check timeout
    int64_t now = current_time_seconds();
    if (snapshot_pb.has_create_at() && snapshot_pb.has_timeout_seconds() &&
        now > snapshot_pb.create_at() + snapshot_pb.timeout_seconds()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot has timed out");
        return;
    }

    // Transition PREPARE → NORMAL
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_NORMAL);
    snapshot_pb.set_finish_at(now);
    if (request.has_image_url()) {
        snapshot_pb.set_image_url(request.image_url());
    }
    if (request.has_last_journal_id()) {
        snapshot_pb.set_last_journal_id(request.last_journal_id());
    }
    if (request.has_snapshot_meta_image_size()) {
        snapshot_pb.set_snapshot_meta_image_size(request.snapshot_meta_image_size());
    }
    if (request.has_snapshot_logical_data_size()) {
        snapshot_pb.set_snapshot_logical_data_size(request.snapshot_logical_data_size());
    }

    write_snapshot_pb(txn.get(), instance_id, vs, snapshot_pb);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
        response->mutable_status()->set_msg("failed to commit commit_snapshot");
        return;
    }

    response->mutable_status()->set_code(MetaServiceCode::OK);
    LOG(INFO) << "commit_snapshot success, instance_id=" << instance_id
              << " snapshot_id=" << request.snapshot_id();
}

// ============================================================================
// abort_snapshot
// ============================================================================

void DorisSnapshotManager::abort_snapshot(std::string_view instance_id,
                                          const AbortSnapshotRequest& request,
                                          AbortSnapshotResponse* response) {
    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("invalid snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create txn");
        return;
    }

    SnapshotPB snapshot_pb;
    err = read_snapshot_pb(txn.get(), instance_id, vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot not found");
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to read snapshot");
        return;
    }

    // Already ABORTED — idempotent
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    // Only PREPARE snapshots can be aborted
    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_PREPARE) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("can only abort PREPARE snapshots, current=" +
                                            std::to_string(static_cast<int>(snapshot_pb.status())));
        return;
    }

    // Transition PREPARE → ABORTED
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
    if (request.has_reason()) {
        snapshot_pb.set_reason(request.reason());
    }

    write_snapshot_pb(txn.get(), instance_id, vs, snapshot_pb);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
        response->mutable_status()->set_msg("failed to commit abort_snapshot");
        return;
    }

    response->mutable_status()->set_code(MetaServiceCode::OK);
    LOG(INFO) << "abort_snapshot success, instance_id=" << instance_id
              << " snapshot_id=" << request.snapshot_id()
              << " reason=" << (request.has_reason() ? request.reason() : "N/A");
}

// ============================================================================
// drop_snapshot
// ============================================================================

void DorisSnapshotManager::drop_snapshot(std::string_view instance_id,
                                         const DropSnapshotRequest& request,
                                         DropSnapshotResponse* response) {
    if (!request.has_snapshot_id() || request.snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot_id must not be empty");
        return;
    }

    Versionstamp vs;
    if (!parse_snapshot_versionstamp(request.snapshot_id(), &vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("invalid snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create txn");
        return;
    }

    SnapshotPB snapshot_pb;
    err = read_snapshot_pb(txn.get(), instance_id, vs, &snapshot_pb);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("snapshot not found");
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to read snapshot");
        return;
    }

    // Already RECYCLED — idempotent
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    // Only NORMAL or ABORTED snapshots can be dropped
    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_NORMAL &&
        snapshot_pb.status() != SnapshotStatus::SNAPSHOT_ABORTED) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                "can only drop NORMAL or ABORTED snapshots, "
                "current status=" +
                std::to_string(static_cast<int>(snapshot_pb.status())));
        return;
    }

    // Mark as RECYCLED (actual cleanup handled by Recycler)
    snapshot_pb.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);

    write_snapshot_pb(txn.get(), instance_id, vs, snapshot_pb);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
        response->mutable_status()->set_msg("failed to commit drop_snapshot");
        return;
    }

    response->mutable_status()->set_code(MetaServiceCode::OK);
    LOG(INFO) << "drop_snapshot success, instance_id=" << instance_id
              << " snapshot_id=" << request.snapshot_id();
}

// ============================================================================
// list_snapshot
// ============================================================================

void DorisSnapshotManager::list_snapshot(std::string_view instance_id,
                                         const ListSnapshotRequest& request,
                                         ListSnapshotResponse* response) {
    // If a specific snapshot is requested
    if (request.has_required_snapshot_id() && !request.required_snapshot_id().empty()) {
        Versionstamp vs;
        if (!parse_snapshot_versionstamp(request.required_snapshot_id(), &vs)) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg("invalid required_snapshot_id format");
            return;
        }

        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv_->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
            response->mutable_status()->set_msg("failed to create txn");
            return;
        }

        SnapshotPB pb;
        err = read_snapshot_pb(txn.get(), instance_id, vs, &pb);
        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
            response->mutable_status()->set_msg("snapshot not found: " +
                                                request.required_snapshot_id());
            return;
        }
        if (err != TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
            response->mutable_status()->set_msg("failed to read snapshot");
            return;
        }

        // Filter by include_aborted
        if (!request.include_aborted() && pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
            response->mutable_status()->set_code(MetaServiceCode::OK);
            return;
        }

        auto* info = response->add_snapshots();
        snapshot_pb_to_info(pb, vs, info);
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    // Range scan all snapshots for this instance
    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string begin_key = encode_versioned_key(key_prefix, Versionstamp::min());
    std::string end_key = encode_versioned_key(key_prefix, Versionstamp::max());

    FullRangeGetOptions opts;
    opts.txn_kv = txn_kv_;
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [key, snapshot_value] = *kvp;

        Versionstamp version;
        std::string_view key_view(key);
        if (!decode_versioned_key(&key_view, &version)) {
            LOG(WARNING) << "list_snapshot: failed to decode versionstamp from key, instance_id="
                         << instance_id << " key=" << hex(key);
            continue;
        }

        SnapshotPB snapshot_pb;
        if (!snapshot_pb.ParseFromArray(snapshot_value.data(), snapshot_value.size())) {
            LOG(WARNING) << "list_snapshot: failed to parse SnapshotPB, instance_id="
                         << instance_id;
            continue;
        }

        // Filter ABORTED snapshots unless explicitly requested
        if (!request.include_aborted() &&
            snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
            continue;
        }
        // Skip RECYCLED snapshots
        if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
            continue;
        }

        auto* info = response->add_snapshots();
        snapshot_pb_to_info(snapshot_pb, version, info);
    }

    if (!it->is_valid()) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("error during snapshot range scan");
        return;
    }

    response->mutable_status()->set_code(MetaServiceCode::OK);
}

// ============================================================================
// clone_instance
// ============================================================================

void DorisSnapshotManager::clone_instance(const CloneInstanceRequest& request,
                                          CloneInstanceResponse* response) {
    // Validate required fields
    if (!request.has_from_snapshot_id() || request.from_snapshot_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("from_snapshot_id must not be empty");
        return;
    }
    if (!request.has_from_instance_id() || request.from_instance_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("from_instance_id must not be empty");
        return;
    }
    if (!request.has_new_instance_id() || request.new_instance_id().empty()) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("new_instance_id must not be empty");
        return;
    }
    if (!request.has_clone_type() || request.clone_type() == CloneInstanceRequest::UNKNOWN) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                "clone_type must be specified (READ_ONLY, WRITABLE, or ROLLBACK)");
        return;
    }

    Versionstamp snapshot_vs;
    if (!parse_snapshot_versionstamp(request.from_snapshot_id(), &snapshot_vs)) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("invalid from_snapshot_id format");
        return;
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
        response->mutable_status()->set_msg("failed to create txn");
        return;
    }

    // 1. Read source snapshot — must be NORMAL
    SnapshotPB source_snapshot;
    err = read_snapshot_pb(txn.get(), request.from_instance_id(), snapshot_vs, &source_snapshot);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("source snapshot not found");
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to read source snapshot");
        return;
    }
    if (source_snapshot.status() != SnapshotStatus::SNAPSHOT_NORMAL) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("source snapshot is not in NORMAL state");
        return;
    }

    // 2. Read source instance info
    InstanceInfoPB source_instance;
    err = read_instance_info(txn.get(), request.from_instance_id(), &source_instance);
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
        response->mutable_status()->set_msg("failed to read source instance info");
        return;
    }

    // 3. Check that the new instance doesn't already exist
    {
        std::string new_key = instance_key({std::string(request.new_instance_id())});
        std::string existing_val;
        err = txn->get(new_key, &existing_val);
        if (err == TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(MetaServiceCode::ALREADY_EXISTED);
            response->mutable_status()->set_msg("new instance already exists");
            return;
        }
        if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_GET_ERR);
            response->mutable_status()->set_msg("failed to check new instance existence");
            return;
        }
    }

    // 4. Build new InstanceInfoPB based on clone_type
    InstanceInfoPB new_instance;
    new_instance.set_instance_id(request.new_instance_id());
    new_instance.set_source_instance_id(request.from_instance_id());
    new_instance.set_source_snapshot_id(request.from_snapshot_id());

    auto clone_type = request.clone_type();
    switch (clone_type) {
    case CloneInstanceRequest::READ_ONLY:
        new_instance.set_ready_only(true);
        // Inherit obj_info from source
        for (const auto& obj : source_instance.obj_info()) {
            new_instance.add_obj_info()->CopyFrom(obj);
        }
        break;

    case CloneInstanceRequest::WRITABLE:
        new_instance.set_ready_only(false);
        // Use new obj_info from request if provided, else inherit
        if (request.has_obj_info()) {
            new_instance.add_obj_info()->CopyFrom(request.obj_info());
        } else {
            for (const auto& obj : source_instance.obj_info()) {
                new_instance.add_obj_info()->CopyFrom(obj);
            }
        }
        break;

    case CloneInstanceRequest::ROLLBACK:
        new_instance.set_ready_only(false);
        // Inherit obj_info from source
        for (const auto& obj : source_instance.obj_info()) {
            new_instance.add_obj_info()->CopyFrom(obj);
        }
        // Set rollback-specific fields
        if (source_instance.has_original_instance_id()) {
            new_instance.set_original_instance_id(source_instance.original_instance_id());
        } else {
            new_instance.set_original_instance_id(request.from_instance_id());
        }
        new_instance.set_successor_instance_id(request.from_instance_id());
        break;

    default:
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("unsupported clone_type");
        return;
    }

    // Inherit common fields from source instance
    if (source_instance.has_user_id()) {
        new_instance.set_user_id(source_instance.user_id());
    }
    if (request.has_snapshot_name()) {
        new_instance.set_name(request.snapshot_name());
    }
    int64_t now_ms = current_time_seconds() * 1000;
    new_instance.set_ctime(now_ms);
    new_instance.set_mtime(now_ms);
    new_instance.set_status(InstanceInfoPB::NORMAL);

    // 5. Write new instance key
    {
        std::string new_key = instance_key({std::string(request.new_instance_id())});
        txn->put(new_key, new_instance.SerializeAsString());
    }

    // 6. Write snapshot reference key (record derivation relationship)
    {
        std::string ref_key = versioned::snapshot_reference_key(
                {std::string(request.from_instance_id()), snapshot_vs,
                 std::string(request.new_instance_id())});
        txn->put(ref_key, ""); // empty value — key itself encodes the relationship
    }

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        response->mutable_status()->set_code(MetaServiceCode::KV_TXN_COMMIT_ERR);
        response->mutable_status()->set_msg("failed to commit clone_instance");
        return;
    }

    // 7. Build response
    response->mutable_status()->set_code(MetaServiceCode::OK);
    if (new_instance.obj_info_size() > 0) {
        response->mutable_obj_info()->CopyFrom(new_instance.obj_info(0));
    }
    if (source_snapshot.has_image_url()) {
        response->set_image_url(source_snapshot.image_url());
    }

    LOG(INFO) << "clone_instance success, from_instance=" << request.from_instance_id()
              << " from_snapshot=" << request.from_snapshot_id()
              << " new_instance=" << request.new_instance_id()
              << " clone_type=" << static_cast<int>(clone_type);
}

// ============================================================================
// Phase 2: Operational Capabilities
// ============================================================================

// --- set_multi_version_status ---

std::pair<MetaServiceCode, std::string> DorisSnapshotManager::set_multi_version_status(
        std::string_view instance_id, MultiVersionStatus multi_version_status) {
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_CREATE_ERR, "failed to create txn"};
    }

    InstanceInfoPB instance;
    err = read_instance_info(txn.get(), instance_id, &instance);
    if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_GET_ERR, "failed to read instance info"};
    }

    // Validate state transition
    auto current = instance.has_multi_version_status() ? instance.multi_version_status()
                                                       : MultiVersionStatus::MULTI_VERSION_DISABLED;
    if (multi_version_status == current) {
        return {MetaServiceCode::OK, "no change"};
    }

    // Only allow forward transitions: DISABLED→WRITE_ONLY→READ_WRITE→ENABLED
    if (static_cast<int>(multi_version_status) < static_cast<int>(current)) {
        return {MetaServiceCode::INVALID_ARGUMENT,
                "cannot transition backward: current=" + std::to_string(static_cast<int>(current)) +
                        " target=" + std::to_string(static_cast<int>(multi_version_status))};
    }

    instance.set_multi_version_status(multi_version_status);
    std::string key = instance_key({std::string(instance_id)});
    txn->put(key, instance.SerializeAsString());

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        return {MetaServiceCode::KV_TXN_COMMIT_ERR, "failed to commit"};
    }

    LOG(INFO) << "set_multi_version_status success, instance_id=" << instance_id
              << " status=" << static_cast<int>(multi_version_status);
    return {MetaServiceCode::OK, ""};
}

// --- recycle_snapshots ---

int DorisSnapshotManager::recycle_snapshots(InstanceRecycler* recycler) {
    std::string_view instance_id = recycler->instance_id();
    LOG(INFO) << "recycle_snapshots start, instance_id=" << instance_id;

    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string begin_key = encode_versioned_key(key_prefix, Versionstamp::min());
    std::string end_key = encode_versioned_key(key_prefix, Versionstamp::max());

    FullRangeGetOptions opts;
    opts.txn_kv = txn_kv_;
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

    int64_t now = current_time_seconds();
    int normal_count = 0;
    int recycled = 0;
    int errors = 0;

    struct SnapshotEntry {
        Versionstamp vs;
        SnapshotPB pb;
    };
    std::vector<SnapshotEntry> normal_entries;
    std::vector<SnapshotEntry> to_recycle;

    // First pass: classify snapshots
    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [key, value] = *kvp;

        Versionstamp vs;
        std::string_view key_view(key);
        if (!decode_versioned_key(&key_view, &vs)) {
            LOG(WARNING) << "recycle_snapshots: failed to decode key, instance_id=" << instance_id;
            continue;
        }

        SnapshotPB pb;
        if (!pb.ParseFromArray(value.data(), value.size())) {
            LOG(WARNING) << "recycle_snapshots: failed to parse SnapshotPB, instance_id="
                         << instance_id;
            continue;
        }

        // Check PREPARE timeout → mark ABORTED
        if (pb.status() == SnapshotStatus::SNAPSHOT_PREPARE) {
            if (pb.has_create_at() && pb.has_timeout_seconds() &&
                now > pb.create_at() + pb.timeout_seconds()) {
                LOG(INFO) << "recycle_snapshots: marking PREPARE-timeout snapshot as ABORTED"
                          << ", instance_id=" << instance_id << ", snapshot_id=" << vs.to_string();
                // Write back ABORTED status
                std::unique_ptr<Transaction> txn;
                if (txn_kv_->create_txn(&txn) == TxnErrorCode::TXN_OK) {
                    SnapshotPB upd = pb;
                    upd.set_status(SnapshotStatus::SNAPSHOT_ABORTED);
                    upd.set_reason("PREPARE timeout");
                    write_snapshot_pb(txn.get(), instance_id, vs, upd);
                    TxnErrorCode ce = txn->commit();
                    if (ce != TxnErrorCode::TXN_OK) {
                        LOG(WARNING) << "recycle_snapshots: failed to commit "
                                     << "ABORTED status, instance_id=" << instance_id
                                     << ", snapshot_id=" << vs.to_string();
                        continue;
                    }
                    pb = std::move(upd);
                }
            }
        }

        // Check TTL expiration for NORMAL snapshots → mark RECYCLED
        if (pb.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
            if (pb.has_ttl_seconds() && pb.ttl_seconds() > 0 && pb.has_create_at() &&
                now > pb.create_at() + pb.ttl_seconds()) {
                LOG(INFO) << "recycle_snapshots: TTL expired, marking RECYCLED"
                          << ", instance_id=" << instance_id << ", snapshot_id=" << vs.to_string();
                // Write back RECYCLED status
                std::unique_ptr<Transaction> txn;
                if (txn_kv_->create_txn(&txn) == TxnErrorCode::TXN_OK) {
                    SnapshotPB upd = pb;
                    upd.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
                    upd.set_reason("TTL expired");
                    write_snapshot_pb(txn.get(), instance_id, vs, upd);
                    TxnErrorCode ce = txn->commit();
                    if (ce != TxnErrorCode::TXN_OK) {
                        LOG(WARNING) << "recycle_snapshots: failed to commit "
                                     << "RECYCLED status, instance_id=" << instance_id
                                     << ", snapshot_id=" << vs.to_string();
                        continue;
                    }
                    pb = std::move(upd);
                }
            }
        }

        // Collect entries for cleanup or counting
        if (pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED ||
            pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
            to_recycle.push_back({vs, std::move(pb)});
        } else if (pb.status() == SnapshotStatus::SNAPSHOT_NORMAL) {
            normal_entries.push_back({vs, std::move(pb)});
            normal_count++;
        }
    }

    if (!it->is_valid()) {
        LOG(WARNING) << "recycle_snapshots: range scan error, instance_id=" << instance_id
                     << ", error=" << it->error_code();
        return -1;
    }

    // Check max_reserved — mark oldest NORMAL snapshots as RECYCLED
    int32_t max_reserved = config::snapshot_max_reserved_num;
    if (recycler->instance_info().has_max_reserved_snapshot()) {
        max_reserved = static_cast<int32_t>(recycler->instance_info().max_reserved_snapshot());
    }
    if (normal_count > max_reserved && max_reserved >= 0) {
        // Sort normal entries by create_at ascending (oldest first)
        std::sort(normal_entries.begin(), normal_entries.end(),
                  [](const SnapshotEntry& a, const SnapshotEntry& b) {
                      return a.pb.create_at() < b.pb.create_at();
                  });
        int to_remove = normal_count - max_reserved;
        for (int i = 0; i < to_remove && i < static_cast<int>(normal_entries.size()); i++) {
            LOG(INFO) << "recycle_snapshots: max_reserved exceeded, marking RECYCLED"
                      << ", instance_id=" << instance_id
                      << ", snapshot_id=" << normal_entries[i].vs.to_string();
            normal_entries[i].pb.set_status(SnapshotStatus::SNAPSHOT_RECYCLED);
            normal_entries[i].pb.set_reason("max_reserved exceeded");
            std::unique_ptr<Transaction> txn;
            if (txn_kv_->create_txn(&txn) == TxnErrorCode::TXN_OK) {
                write_snapshot_pb(txn.get(), instance_id, normal_entries[i].vs,
                                  normal_entries[i].pb);
                TxnErrorCode ce = txn->commit();
                if (ce != TxnErrorCode::TXN_OK) {
                    LOG(WARNING) << "recycle_snapshots: failed to commit "
                                 << "max_reserved RECYCLED, instance_id=" << instance_id
                                 << ", snapshot_id=" << normal_entries[i].vs.to_string();
                    continue;
                }
            }
            to_recycle.push_back(std::move(normal_entries[i]));
        }
    }

    // Execute actual cleanup for RECYCLED/ABORTED snapshots
    for (auto& entry : to_recycle) {
        std::string resource_id;
        if (entry.pb.has_resource_id()) {
            resource_id = entry.pb.resource_id();
        } else if (recycler->instance_info().resource_ids_size() > 0) {
            resource_id = recycler->instance_info().resource_ids(0);
        }

        int ret = recycler->recycle_snapshot_meta_and_data(resource_id, entry.vs, entry.pb);
        if (ret != 0) {
            LOG(WARNING) << "recycle_snapshots: failed to recycle snapshot"
                         << ", instance_id=" << instance_id
                         << ", snapshot_id=" << entry.vs.to_string() << ", ret=" << ret;
            errors++;
        } else {
            recycled++;
        }
    }

    LOG(INFO) << "recycle_snapshots done, instance_id=" << instance_id << " recycled=" << recycled
              << " errors=" << errors;
    return errors > 0 ? -1 : 0;
}

// --- recycle_snapshot_meta_and_data ---

int DorisSnapshotManager::recycle_snapshot_meta_and_data(std::string_view instance_id,
                                                         std::string_view resource_id,
                                                         StorageVaultAccessor* accessor,
                                                         Versionstamp snapshot_version,
                                                         const SnapshotPB& snapshot_pb) {
    // 1. Delete object storage files (image directory)
    if (accessor != nullptr && snapshot_pb.has_image_url() && !snapshot_pb.image_url().empty()) {
        int ret = accessor->delete_directory(snapshot_pb.image_url());
        if (ret != 0) {
            LOG(WARNING) << "recycle_snapshot_meta_and_data: failed to delete snapshot directory"
                         << ", instance_id=" << instance_id
                         << ", image_url=" << snapshot_pb.image_url() << ", ret=" << ret;
            // Continue to delete KV metadata anyway — object storage is best-effort
        }
    }

    // 2. Abort residual multipart upload if present
    if (accessor != nullptr && snapshot_pb.has_upload_file() && snapshot_pb.has_upload_id() &&
        !snapshot_pb.upload_file().empty() && !snapshot_pb.upload_id().empty()) {
        accessor->abort_multipart_upload(snapshot_pb.upload_file(), snapshot_pb.upload_id());
    }

    // 3. Delete snapshot_full_key from TxnKv
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "recycle_snapshot_meta_and_data: failed to create txn"
                     << ", instance_id=" << instance_id;
        return -1;
    }

    std::string snapshot_key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string snapshot_full_key = encode_versioned_key(snapshot_key_prefix, snapshot_version);
    txn->remove(snapshot_full_key);

    // 4. Delete associated snapshot_reference_keys
    //    Reference keys have prefix: snapshot_reference_key_prefix(instance_id, versionstamp)
    std::string ref_prefix =
            versioned::snapshot_reference_key_prefix(instance_id, snapshot_version);
    std::string ref_end = ref_prefix;
    // Safe next-prefix: handle trailing 0xFF carry
    while (!ref_end.empty() && static_cast<unsigned char>(ref_end.back()) == 0xFF) {
        ref_end.pop_back();
    }
    if (!ref_end.empty()) {
        ref_end.back() = ref_end.back() + 1;
    }
    // Range-delete all reference keys for this snapshot
    txn->remove(ref_prefix, ref_end);

    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "recycle_snapshot_meta_and_data: failed to commit KV deletion"
                     << ", instance_id=" << instance_id
                     << ", snapshot_id=" << snapshot_version.to_string();
        return -1;
    }

    LOG(INFO) << "recycle_snapshot_meta_and_data success, instance_id=" << instance_id
              << ", snapshot_id=" << snapshot_version.to_string();
    return 0;
}

// ============================================================================
// Phase 2: Consistency Checking
// ============================================================================

// --- check_snapshots ---
// Forward check: TxnKv snapshot keys → verify image files exist in object storage.
// Returns 0=success, 1=file lost (data loss), negative=error.

int DorisSnapshotManager::check_snapshots(InstanceChecker* checker) {
    std::string_view instance_id = checker->instance_id();
    LOG(INFO) << "check_snapshots start, instance_id=" << instance_id;

    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string begin_key = encode_versioned_key(key_prefix, Versionstamp::min());
    std::string end_key = encode_versioned_key(key_prefix, Versionstamp::max());

    FullRangeGetOptions opts;
    opts.txn_kv = txn_kv_;
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

    int result = 0;
    int checked = 0;

    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [key, value] = *kvp;

        Versionstamp vs;
        std::string_view key_view(key);
        if (!decode_versioned_key(&key_view, &vs)) continue;

        SnapshotPB pb;
        if (!pb.ParseFromArray(value.data(), value.size())) continue;

        // Only verify NORMAL snapshots that have image URLs
        if (pb.status() != SnapshotStatus::SNAPSHOT_NORMAL) continue;
        if (!pb.has_image_url() || pb.image_url().empty()) continue;

        // Get accessor for the snapshot's resource
        std::string resource_id = pb.has_resource_id() ? pb.resource_id() : "";
        StorageVaultAccessor* accessor = nullptr;
        if (!resource_id.empty()) accessor = checker->get_accessor(resource_id);
        if (!accessor) {
            std::vector<StorageVaultAccessor*> all;
            checker->get_all_accessor(&all);
            if (!all.empty()) accessor = all[0];
        }
        if (!accessor) {
            LOG(WARNING) << "check_snapshots: no accessor, instance_id=" << instance_id
                         << ", snapshot_id=" << vs.to_string();
            return -1;
        }

        // Verify snapshot image exists in object storage
        int ret = accessor->exists(pb.image_url());
        if (ret == 1) {
            LOG(WARNING) << "check_snapshots: snapshot file LOST"
                         << ", instance_id=" << instance_id << ", snapshot_id=" << vs.to_string()
                         << ", image_url=" << pb.image_url();
            result = 1;
        } else if (ret < 0) {
            LOG(WARNING) << "check_snapshots: error checking existence"
                         << ", instance_id=" << instance_id << ", snapshot_id=" << vs.to_string();
            return -1;
        }
        checked++;
    }

    if (!it->is_valid()) {
        LOG(WARNING) << "check_snapshots: range scan error, instance_id=" << instance_id;
        return -1;
    }

    LOG(INFO) << "check_snapshots done, instance_id=" << instance_id << ", checked=" << checked
              << ", result=" << result;
    return result;
}

// --- inverted_check_snapshots ---
// Inverse check: list object storage snapshot files → verify TxnKv keys exist.
// Returns 0=success, 1=file leaked (orphan file), negative=error.

int DorisSnapshotManager::inverted_check_snapshots(InstanceChecker* checker) {
    std::string_view instance_id = checker->instance_id();
    LOG(INFO) << "inverted_check_snapshots start, instance_id=" << instance_id;

    // Step 1: Collect known snapshot image URLs from TxnKv
    std::unordered_set<std::string> known_urls;
    {
        std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
        std::string begin_key = encode_versioned_key(key_prefix, Versionstamp::min());
        std::string end_key = encode_versioned_key(key_prefix, Versionstamp::max());

        FullRangeGetOptions opts;
        opts.txn_kv = txn_kv_;
        opts.prefetch = true;
        auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

        for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto&& [key, value] = *kvp;
            SnapshotPB pb;
            if (!pb.ParseFromArray(value.data(), value.size())) continue;
            if (pb.has_image_url() && !pb.image_url().empty()) {
                known_urls.insert(pb.image_url());
            }
        }
        if (!it->is_valid()) {
            LOG(WARNING) << "inverted_check_snapshots: range scan error";
            return -1;
        }
    }

    // Step 2: List snapshot files in object storage, check for orphans
    int result = 0;
    std::vector<StorageVaultAccessor*> accessors;
    checker->get_all_accessor(&accessors);

    for (auto* accessor : accessors) {
        if (!accessor) continue;
        std::string snapshot_dir = "snapshot/" + std::string(instance_id);
        std::unique_ptr<ListIterator> list_it;
        int ret = accessor->list_directory(snapshot_dir, &list_it);
        if (ret != 0 || !list_it) continue; // Dir may not exist

        while (list_it->is_valid() && list_it->has_next()) {
            auto file_meta = list_it->next();
            if (!file_meta.has_value()) break;

            if (known_urls.find(file_meta->path) == known_urls.end()) {
                LOG(WARNING) << "inverted_check_snapshots: file LEAKED"
                             << ", instance_id=" << instance_id << ", path=" << file_meta->path;
                result = 1;
            }
        }
    }

    LOG(INFO) << "inverted_check_snapshots done, instance_id=" << instance_id
              << ", known_urls=" << known_urls.size() << ", result=" << result;
    return result;
}

// --- check_mvcc_meta_key ---
// Forward check: verify versioned snapshot keys and reference keys are structurally consistent.
// Returns 0=success, 1=key malformed, negative=error.

int DorisSnapshotManager::check_mvcc_meta_key(InstanceChecker* checker) {
    std::string_view instance_id = checker->instance_id();
    LOG(INFO) << "check_mvcc_meta_key start, instance_id=" << instance_id;

    // Validate all versioned snapshot keys can be decoded and parsed
    std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
    std::string begin_key = encode_versioned_key(key_prefix, Versionstamp::min());
    std::string end_key = encode_versioned_key(key_prefix, Versionstamp::max());

    FullRangeGetOptions opts;
    opts.txn_kv = txn_kv_;
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

    int result = 0;
    int checked = 0;

    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [key, value] = *kvp;

        Versionstamp vs;
        std::string_view key_view(key);
        if (!decode_versioned_key(&key_view, &vs)) {
            LOG(WARNING) << "check_mvcc_meta_key: malformed key, instance_id=" << instance_id;
            result = 1;
            continue;
        }

        SnapshotPB pb;
        if (!pb.ParseFromArray(value.data(), value.size())) {
            LOG(WARNING) << "check_mvcc_meta_key: malformed value, instance_id=" << instance_id
                         << ", snapshot_id=" << vs.to_string();
            result = 1;
            continue;
        }
        checked++;
    }

    if (!it->is_valid()) {
        LOG(WARNING) << "check_mvcc_meta_key: scan error, instance_id=" << instance_id;
        return -1;
    }

    // Also validate snapshot reference keys
    std::string ref_prefix = versioned::snapshot_reference_key_prefix(instance_id);
    std::string ref_end = ref_prefix + '\xFF';

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) return -1;

    std::unique_ptr<RangeGetIterator> ref_it;
    err = txn->get(ref_prefix, ref_end, &ref_it);
    if (err != TxnErrorCode::TXN_OK) return -1;

    int ref_count = 0;
    while (ref_it->has_next()) {
        auto [rkey, rval] = ref_it->next();
        ref_count++;
    }

    LOG(INFO) << "check_mvcc_meta_key done, instance_id=" << instance_id
              << ", snapshots=" << checked << ", refs=" << ref_count << ", result=" << result;
    return result;
}

// --- inverted_check_mvcc_meta_key ---
// Inverse check: verify snapshot reference keys have valid targets.
// Returns 0=success, 1=orphan reference, negative=error.

int DorisSnapshotManager::inverted_check_mvcc_meta_key(InstanceChecker* checker) {
    std::string_view instance_id = checker->instance_id();
    LOG(INFO) << "inverted_check_mvcc_meta_key start, instance_id=" << instance_id;

    // Collect all valid snapshot IDs for this instance
    std::unordered_set<std::string> valid_snapshots;
    {
        std::string key_prefix = versioned::snapshot_full_key({std::string(instance_id)});
        std::string begin_key = encode_versioned_key(key_prefix, Versionstamp::min());
        std::string end_key = encode_versioned_key(key_prefix, Versionstamp::max());

        FullRangeGetOptions opts;
        opts.txn_kv = txn_kv_;
        opts.prefetch = true;
        auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

        for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto&& [key, value] = *kvp;
            Versionstamp vs;
            std::string_view key_view(key);
            if (decode_versioned_key(&key_view, &vs)) {
                valid_snapshots.insert(vs.to_string());
            }
        }
        if (!it->is_valid()) {
            LOG(WARNING) << "inverted_check_mvcc_meta_key: snapshot scan error";
            return -1;
        }
    }

    // Scan reference keys and verify integrity
    int result = 0;
    std::string ref_prefix = versioned::snapshot_reference_key_prefix(instance_id);
    std::string ref_end = ref_prefix + '\xFF';

    FullRangeGetOptions ref_opts;
    ref_opts.txn_kv = txn_kv_;
    ref_opts.prefetch = true;
    auto it = txn_kv_->full_range_get(ref_prefix, ref_end, ref_opts);

    // Length of the instance-level reference key prefix, used
    // to skip past it and decode the embedded versionstamp.
    size_t prefix_len = ref_prefix.size();

    int ref_count = 0;
    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [key, value] = *kvp;
        ref_count++;

        // Extract the snapshot versionstamp embedded after the
        // instance-level prefix in the reference key.
        if (key.size() > prefix_len) {
            std::string_view after_prefix(key.data() + prefix_len, key.size() - prefix_len);
            Versionstamp ref_vs;
            if (decode_versionstamp(&after_prefix, &ref_vs) == 0) {
                if (valid_snapshots.find(ref_vs.to_string()) == valid_snapshots.end()) {
                    LOG(WARNING) << "inverted_check_mvcc_meta_key: "
                                 << "orphan reference key, instance=" << instance_id
                                 << ", ref_vs=" << ref_vs.to_string();
                    result = 1;
                }
            }
        }
    }
    if (!it->is_valid()) {
        LOG(WARNING) << "inverted_check_mvcc_meta_key: reference scan error";
        return -1;
    }

    LOG(INFO) << "inverted_check_mvcc_meta_key done, instance_id=" << instance_id
              << ", valid_snapshots=" << valid_snapshots.size() << ", refs=" << ref_count
              << ", result=" << result;
    return result;
}

// --- check_meta ---
// MetaChecker callback: verify snapshot metadata integrity.
// Returns 0=success, 1=malformed entries found, negative=error.

int DorisSnapshotManager::check_meta(MetaChecker* meta_checker) {
    std::string instance_id = meta_checker->instance_id();
    LOG(INFO) << "check_meta start, instance_id=" << instance_id;

    std::string key_prefix = versioned::snapshot_full_key({instance_id});
    std::string begin_key = encode_versioned_key(key_prefix, Versionstamp::min());
    std::string end_key = encode_versioned_key(key_prefix, Versionstamp::max());

    FullRangeGetOptions opts;
    opts.txn_kv = txn_kv_;
    opts.prefetch = true;
    auto it = txn_kv_->full_range_get(begin_key, end_key, opts);

    int snapshot_count = 0;
    int malformed = 0;

    for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
        auto&& [key, value] = *kvp;

        Versionstamp vs;
        std::string_view key_view(key);
        if (!decode_versioned_key(&key_view, &vs)) {
            malformed++;
            continue;
        }

        SnapshotPB pb;
        if (!pb.ParseFromArray(value.data(), value.size())) {
            malformed++;
            continue;
        }
        snapshot_count++;
    }

    if (!it->is_valid()) {
        LOG(WARNING) << "check_meta: scan error, instance_id=" << instance_id;
        return -1;
    }

    if (malformed > 0) {
        LOG(WARNING) << "check_meta: malformed snapshot entries, instance_id=" << instance_id
                     << ", malformed=" << malformed;
    }

    LOG(INFO) << "check_meta done, instance_id=" << instance_id
              << ", snapshot_count=" << snapshot_count << ", malformed=" << malformed;
    return malformed > 0 ? 1 : 0;
}

// ============================================================================
// Phase 2: Data Migration & Chain Compaction
// ============================================================================

// --- migrate_to_versioned_keys ---
// Migrate single-version metadata keys to versioned (MVCC) format.
// This prepares the instance for multi-version/snapshot support.
// Returns 0=success, negative=error.

int DorisSnapshotManager::migrate_to_versioned_keys(InstanceDataMigrator* migrator) {
    std::string_view instance_id = migrator->instance_id();
    LOG(INFO) << "migrate_to_versioned_keys start, instance_id=" << instance_id;

    const auto& instance_info = migrator->instance_info();
    if (instance_info.multi_version_status() == MultiVersionStatus::MULTI_VERSION_DISABLED) {
        LOG(INFO) << "migrate_to_versioned_keys: multi_version disabled, skip";
        return 0;
    }

    const int BATCH_SIZE = 200;
    int total_migrated = 0;

    // Phase 1: Migrate tablet metadata keys
    //   non-versioned: meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id})
    //   versioned:     versioned::meta_tablet_key({instance_id, tablet_id}) + versionstamp
    {
        std::string begin = meta_tablet_key({std::string(instance_id), 0, 0, 0, 0});
        std::string end = meta_tablet_key(
                {std::string(instance_id), INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX});

        FullRangeGetOptions opts;
        opts.txn_kv = txn_kv_;
        opts.prefetch = true;
        auto it = txn_kv_->full_range_get(begin, end, opts);

        std::unique_ptr<Transaction> txn;
        int batch_count = 0;

        for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto&& [key, value] = *kvp;

            if (!txn || batch_count >= BATCH_SIZE) {
                if (txn && batch_count > 0) {
                    if (txn->commit() != TxnErrorCode::TXN_OK) {
                        LOG(WARNING) << "migrate_to_versioned_keys: tablet commit failed";
                        return -1;
                    }
                }
                if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) return -1;
                batch_count = 0;
            }

            // Decode non-versioned tablet key to extract tablet_id
            std::string_view key_view(key);
            int64_t table_id = 0, index_id = 0, partition_id = 0, tablet_id = 0;
            if (decode_meta_tablet_key(&key_view, &table_id, &index_id, &partition_id,
                                       &tablet_id)) {
                std::string vkey =
                        versioned::meta_tablet_key({std::string(instance_id), tablet_id});
                txn->versioned_put(vkey, std::string(value));
                batch_count++;
                total_migrated++;
            }
        }

        if (txn && batch_count > 0) {
            if (txn->commit() != TxnErrorCode::TXN_OK) {
                LOG(WARNING) << "migrate_to_versioned_keys: final tablet commit failed";
                return -1;
            }
        }
        if (!it->is_valid()) {
            LOG(WARNING) << "migrate_to_versioned_keys: tablet scan error";
            return -1;
        }
    }

    // Phase 2: Migrate rowset metadata keys
    //   non-versioned: meta_rowset_key({instance_id, tablet_id, version})
    //   versioned:     versioned::meta_rowset_key({instance_id, tablet_id, version}) + versionstamp
    {
        std::string begin = meta_rowset_key({std::string(instance_id), 0, 0});
        std::string end = meta_rowset_key({std::string(instance_id), INT64_MAX, INT64_MAX});

        FullRangeGetOptions opts;
        opts.txn_kv = txn_kv_;
        opts.prefetch = true;
        auto it = txn_kv_->full_range_get(begin, end, opts);

        std::unique_ptr<Transaction> txn;
        int batch_count = 0;

        for (auto&& kvp = it->next(); kvp.has_value(); kvp = it->next()) {
            auto&& [key, value] = *kvp;

            if (!txn || batch_count >= BATCH_SIZE) {
                if (txn && batch_count > 0) {
                    if (txn->commit() != TxnErrorCode::TXN_OK) {
                        LOG(WARNING) << "migrate_to_versioned_keys: rowset commit failed";
                        return -1;
                    }
                }
                if (txn_kv_->create_txn(&txn) != TxnErrorCode::TXN_OK) return -1;
                batch_count = 0;
            }

            std::string_view key_view(key);
            int64_t tablet_id = 0, version = 0;
            if (decode_meta_rowset_key(&key_view, &tablet_id, &version)) {
                std::string vkey =
                        versioned::meta_rowset_key({std::string(instance_id), tablet_id, version});
                txn->versioned_put(vkey, std::string(value));
                batch_count++;
                total_migrated++;
            }
        }

        if (txn && batch_count > 0) {
            if (txn->commit() != TxnErrorCode::TXN_OK) {
                LOG(WARNING) << "migrate_to_versioned_keys: final rowset commit failed";
                return -1;
            }
        }
        if (!it->is_valid()) {
            LOG(WARNING) << "migrate_to_versioned_keys: rowset scan error";
            return -1;
        }
    }

    LOG(INFO) << "migrate_to_versioned_keys done, instance_id=" << instance_id
              << ", total_migrated=" << total_migrated;
    return 0;
}

// --- compact_snapshot_chains ---
// Compact snapshot reference chains for a cloned instance.
// Validates the chain and prepares for handle_compaction_completion().
// Returns 0=success, negative=error.

int DorisSnapshotManager::compact_snapshot_chains(InstanceChainCompactor* compactor) {
    std::string_view instance_id = compactor->instance_id();
    LOG(INFO) << "compact_snapshot_chains start, instance_id=" << instance_id;

    const auto& instance_info = compactor->instance_info();

    // Verify this instance was cloned from a snapshot
    if (!instance_info.has_source_instance_id() || instance_info.source_instance_id().empty() ||
        !instance_info.has_source_snapshot_id() || instance_info.source_snapshot_id().empty()) {
        LOG(INFO) << "compact_snapshot_chains: not a cloned instance, skip";
        return 0;
    }

    const std::string& source_instance_id = instance_info.source_instance_id();
    const std::string& source_snapshot_id = instance_info.source_snapshot_id();

    Versionstamp snapshot_vs;
    if (!SnapshotManager::parse_snapshot_versionstamp(source_snapshot_id, &snapshot_vs)) {
        LOG(WARNING) << "compact_snapshot_chains: invalid snapshot_id=" << source_snapshot_id;
        return -1;
    }

    LOG(INFO) << "compact_snapshot_chains: processing chain"
              << ", instance_id=" << instance_id << ", source_instance=" << source_instance_id
              << ", source_snapshot=" << source_snapshot_id;

    // Verify the source snapshot still exists
    std::string src_key_prefix = versioned::snapshot_full_key({source_instance_id});
    std::string src_snapshot_key = encode_versioned_key(src_key_prefix, snapshot_vs);

    std::unique_ptr<Transaction> read_txn;
    if (txn_kv_->create_txn(&read_txn) != TxnErrorCode::TXN_OK) return -1;

    std::string snapshot_value;
    TxnErrorCode err = read_txn->get(src_snapshot_key, &snapshot_value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "compact_snapshot_chains: source snapshot not found"
                     << ", source_instance=" << source_instance_id
                     << ", snapshot_id=" << source_snapshot_id;
        return -1;
    }

    SnapshotPB source_snapshot;
    if (!source_snapshot.ParseFromString(snapshot_value)) {
        LOG(WARNING) << "compact_snapshot_chains: failed to parse source snapshot";
        return -1;
    }

    // Verify source snapshot is in a valid state
    if (source_snapshot.status() != SnapshotStatus::SNAPSHOT_NORMAL) {
        LOG(WARNING) << "compact_snapshot_chains: source snapshot not NORMAL"
                     << ", status=" << static_cast<int>(source_snapshot.status());
        return -1;
    }

    // Verify the reference key for this clone exists
    versioned::SnapshotReferenceKeyInfo ref_info {source_instance_id, snapshot_vs,
                                                  std::string(instance_id)};
    std::string ref_key = versioned::snapshot_reference_key(ref_info);

    std::string ref_value;
    err = read_txn->get(ref_key, &ref_value);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "compact_snapshot_chains: reference key not found"
                     << ", instance_id=" << instance_id
                     << ", source_instance=" << source_instance_id;
        return -1;
    }

    LOG(INFO) << "compact_snapshot_chains done, instance_id=" << instance_id
              << ", source=" << source_instance_id
              << ", snapshot_status=" << static_cast<int>(source_snapshot.status());
    return 0;
}

} // namespace doris::cloud
