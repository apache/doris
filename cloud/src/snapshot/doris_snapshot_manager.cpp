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

#include "common/config.h"
#include "common/util.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/checker.h"
#include "recycler/recycler.h"

namespace doris::cloud {

static int64_t current_time_seconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

// ============================================================================
// Constructor
// ============================================================================

DorisSnapshotManager::DorisSnapshotManager(std::shared_ptr<TxnKv> txn_kv)
        : SnapshotManager(txn_kv), txn_kv_(std::move(txn_kv)) {}

// ============================================================================
// Internal helpers
// ============================================================================

TxnErrorCode DorisSnapshotManager::read_snapshot_pb(Transaction* txn,
                                                     std::string_view instance_id,
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
    std::string snapshot_id = serialize_snapshot_id(vs);
    std::string image_url = build_image_url(instance, snapshot_id);

    // Update the SnapshotPB with image_url in a new txn
    {
        std::unique_ptr<Transaction> txn2;
        err = txn_kv_->create_txn(&txn2);
        if (err != TxnErrorCode::TXN_OK) {
            response->mutable_status()->set_code(MetaServiceCode::KV_TXN_CREATE_ERR);
            response->mutable_status()->set_msg("failed to create txn for image_url update");
            return;
        }
        snapshot_pb.set_image_url(image_url);
        write_snapshot_pb(txn2.get(), instance_id, vs, snapshot_pb);
        err = txn2->commit();
        if (err != TxnErrorCode::TXN_OK) {
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
        response->mutable_status()->set_msg(
                "snapshot is not in PREPARE state, current status=" +
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
        response->mutable_status()->set_msg(
                "snapshot is not in PREPARE state, current status=" +
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

    // Already aborted or recycled — idempotent
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
        response->mutable_status()->set_code(MetaServiceCode::OK);
        return;
    }

    // Transition → ABORTED
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

    // Only NORMAL or ABORTED snapshots can be dropped
    if (snapshot_pb.status() != SnapshotStatus::SNAPSHOT_NORMAL &&
        snapshot_pb.status() != SnapshotStatus::SNAPSHOT_ABORTED) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg(
                "can only drop NORMAL or ABORTED snapshots, current status=" +
                std::to_string(static_cast<int>(snapshot_pb.status())));
        return;
    }

    // Already recycled — idempotent
    if (snapshot_pb.status() == SnapshotStatus::SNAPSHOT_RECYCLED) {
        response->mutable_status()->set_code(MetaServiceCode::OK);
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
            response->mutable_status()->set_msg("snapshot not found: " + request.required_snapshot_id());
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
            LOG(WARNING) << "list_snapshot: failed to parse SnapshotPB, instance_id=" << instance_id;
            continue;
        }

        // Filter ABORTED snapshots unless explicitly requested
        if (!request.include_aborted() && snapshot_pb.status() == SnapshotStatus::SNAPSHOT_ABORTED) {
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
    if (!request.has_clone_type() ||
        request.clone_type() == CloneInstanceRequest::UNKNOWN) {
        response->mutable_status()->set_code(MetaServiceCode::INVALID_ARGUMENT);
        response->mutable_status()->set_msg("clone_type must be specified (READ_ONLY, WRITABLE, or ROLLBACK)");
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
    new_instance.set_ctime(current_time_seconds() * 1000); // ms
    new_instance.set_mtime(current_time_seconds() * 1000);
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
// Phase 2 stubs — return base-class defaults for now
// ============================================================================

std::pair<MetaServiceCode, std::string> DorisSnapshotManager::set_multi_version_status(
        std::string_view instance_id, MultiVersionStatus multi_version_status) {
    // TODO: Phase 2 implementation
    return {MetaServiceCode::UNDEFINED_ERR, "Not implemented yet"};
}

int DorisSnapshotManager::check_snapshots(InstanceChecker* checker) {
    // TODO: Phase 2 implementation
    return 0;
}

int DorisSnapshotManager::inverted_check_snapshots(InstanceChecker* checker) {
    return 0;
}

int DorisSnapshotManager::check_mvcc_meta_key(InstanceChecker* checker) {
    return 0;
}

int DorisSnapshotManager::inverted_check_mvcc_meta_key(InstanceChecker* checker) {
    return 0;
}

int DorisSnapshotManager::check_meta(MetaChecker* meta_checker) {
    return 0;
}

int DorisSnapshotManager::recycle_snapshots(InstanceRecycler* recycler) {
    // TODO: Phase 2 implementation
    return 0;
}

int DorisSnapshotManager::recycle_snapshot_meta_and_data(std::string_view instance_id,
                                                          std::string_view resource_id,
                                                          StorageVaultAccessor* accessor,
                                                          Versionstamp snapshot_version,
                                                          const SnapshotPB& snapshot_pb) {
    // TODO: Phase 2 implementation
    return 0;
}

int DorisSnapshotManager::migrate_to_versioned_keys(InstanceDataMigrator* migrator) {
    LOG(WARNING) << "DorisSnapshotManager::migrate_to_versioned_keys not yet implemented";
    return -1;
}

int DorisSnapshotManager::compact_snapshot_chains(InstanceChainCompactor* compactor) {
    LOG(WARNING) << "DorisSnapshotManager::compact_snapshot_chains not yet implemented";
    return -1;
}

} // namespace doris::cloud
