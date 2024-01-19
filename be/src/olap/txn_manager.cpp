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

#include "txn_manager.h"

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <time.h>

#include <filesystem>
#include <iterator>
#include <list>
#include <new>
#include <ostream>
#include <queue>
#include <set>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "olap/data_dir.h"
#include "olap/delta_writer.h"
#include "olap/olap_common.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/schema_change.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/task/engine_publish_version_task.h"
#include "util/debug_points.h"
#include "util/time.h"

namespace doris {
class OlapMeta;
} // namespace doris

using std::map;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {
using namespace ErrorCode;

struct TabletTxnInfo {
    PUniqueId load_id;
    RowsetSharedPtr rowset;
    PendingRowsetGuard pending_rs_guard;
    bool unique_key_merge_on_write {false};
    DeleteBitmapPtr delete_bitmap;
    // records rowsets calc in commit txn
    RowsetIdUnorderedSet rowset_ids;
    int64_t creation_time;
    bool ingest {false};
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    TxnState state {TxnState::PREPARED};

    TabletTxnInfo() = default;

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset)
            : load_id(load_id), rowset(rowset), creation_time(UnixSeconds()) {}

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset, bool ingest_arg)
            : load_id(load_id), rowset(rowset), creation_time(UnixSeconds()), ingest(ingest_arg) {}

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset, bool merge_on_write,
                  DeleteBitmapPtr delete_bitmap, const RowsetIdUnorderedSet& ids)
            : load_id(load_id),
              rowset(rowset),
              unique_key_merge_on_write(merge_on_write),
              delete_bitmap(delete_bitmap),
              rowset_ids(ids),
              creation_time(UnixSeconds()) {}

    void prepare() { state = TxnState::PREPARED; }
    void commit() { state = TxnState::COMMITTED; }
    void rollback() { state = TxnState::ROLLEDBACK; }
    void abort() {
        if (state == TxnState::PREPARED) {
            state = TxnState::ABORTED;
        }
    }
};

TxnManager::TxnManager(StorageEngine& engine, int32_t txn_map_shard_size, int32_t txn_shard_size)
        : _engine(engine),
          _txn_map_shard_size(txn_map_shard_size),
          _txn_shard_size(txn_shard_size) {
    DCHECK_GT(_txn_map_shard_size, 0);
    DCHECK_GT(_txn_shard_size, 0);
    DCHECK_EQ(_txn_map_shard_size & (_txn_map_shard_size - 1), 0);
    DCHECK_EQ(_txn_shard_size & (_txn_shard_size - 1), 0);
    _txn_map_locks = new std::shared_mutex[_txn_map_shard_size];
    _txn_tablet_maps = new txn_tablet_map_t[_txn_map_shard_size];
    _txn_partition_maps = new txn_partition_map_t[_txn_map_shard_size];
    _txn_mutex = new std::shared_mutex[_txn_shard_size];
    _txn_tablet_delta_writer_map = new txn_tablet_delta_writer_map_t[_txn_map_shard_size];
    _txn_tablet_delta_writer_map_locks = new std::shared_mutex[_txn_map_shard_size];
    // For debugging
    _tablet_version_cache = std::make_unique<TabletVersionCache>(100000);
}

// prepare txn should always be allowed because ingest task will be retried
// could not distinguish rollup, schema change or base table, prepare txn successfully will allow
// ingest retried
Status TxnManager::prepare_txn(TPartitionId partition_id, const Tablet& tablet,
                               TTransactionId transaction_id, const PUniqueId& load_id,
                               bool ingest) {
    return prepare_txn(partition_id, transaction_id, tablet.tablet_id(), tablet.tablet_uid(),
                       load_id, ingest);
}

// most used for ut
Status TxnManager::prepare_txn(TPartitionId partition_id, TTransactionId transaction_id,
                               TTabletId tablet_id, TabletUid tablet_uid, const PUniqueId& load_id,
                               bool ingest) {
    TxnKey key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);
    std::lock_guard<std::shared_mutex> txn_wrlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);

    DBUG_EXECUTE_IF("TxnManager.prepare_txn.random_failed", {
        if (rand() % 100 < (100 * dp->param("percent", 0.5))) {
            LOG_WARNING("TxnManager.prepare_txn.random_failed random failed")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id);
            return Status::InternalError("debug prepare txn random failed");
        }
    });
    DBUG_EXECUTE_IF("TxnManager.prepare_txn.wait", {
        if (auto wait = dp->param<int>("duration", 0); wait > 0) {
            LOG_WARNING("TxnManager.prepare_txn.wait")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id)
                    .tag("wait ms", wait);
            std::this_thread::sleep_for(std::chrono::milliseconds(wait));
        }
    });

    /// Step 1: check if the transaction is already exist
    do {
        auto iter = txn_tablet_map.find(key);
        if (iter == txn_tablet_map.end()) {
            break;
        }

        // exist TxnKey
        auto& txn_tablet_info_map = iter->second;
        auto load_itr = txn_tablet_info_map.find(tablet_info);
        if (load_itr == txn_tablet_info_map.end()) {
            break;
        }

        // found load for txn,tablet
        auto& load_info = load_itr->second;
        // case 1: user commit rowset, then the load id must be equal
        // check if load id is equal
        auto& load_id = load_info->load_id;
        if (load_info->load_id.hi() == load_id.hi() && load_info->load_id.lo() == load_id.lo() &&
            load_info->rowset != nullptr) {
            LOG(WARNING) << "find transaction exists when add to engine."
                         << "partition_id: " << key.first << ", transaction_id: " << key.second
                         << ", tablet: " << tablet_info.to_string();
            return Status::OK();
        }
    } while (false);

    /// Step 2: check if there are too many transactions on running.
    // check if there are too many transactions on running.
    // if yes, reject the request.
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    if (txn_partition_map.size() > config::max_runnings_transactions_per_txn_map) {
        return Status::Error<TOO_MANY_TRANSACTIONS>("too many transactions: {}, limit: {}",
                                                    txn_tablet_map.size(),
                                                    config::max_runnings_transactions_per_txn_map);
    }

    /// Step 3: Add transaction to engine
    // not found load id
    // case 1: user start a new txn, rowset = null
    // case 2: loading txn from meta env
    auto load_info = std::make_shared<TabletTxnInfo>(load_id, nullptr, ingest);
    load_info->prepare();
    txn_tablet_map[key][tablet_info] = std::move(load_info);
    _insert_txn_partition_map_unlocked(transaction_id, partition_id);
    VLOG_NOTICE << "add transaction to engine successfully."
                << "partition_id: " << key.first << ", transaction_id: " << key.second
                << ", tablet: " << tablet_info.to_string();
    return Status::OK();
}

Status TxnManager::commit_txn(TPartitionId partition_id, const Tablet& tablet,
                              TTransactionId transaction_id, const PUniqueId& load_id,
                              const RowsetSharedPtr& rowset_ptr, PendingRowsetGuard guard,
                              bool is_recovery) {
    return commit_txn(tablet.data_dir()->get_meta(), partition_id, transaction_id,
                      tablet.tablet_id(), tablet.tablet_uid(), load_id, rowset_ptr,
                      std::move(guard), is_recovery);
}

Status TxnManager::publish_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                               TTransactionId transaction_id, const Version& version,
                               TabletPublishStatistics* stats) {
    return publish_txn(tablet->data_dir()->get_meta(), partition_id, transaction_id,
                       tablet->tablet_id(), tablet->tablet_uid(), version, stats);
}

void TxnManager::abort_txn(TPartitionId partition_id, TTransactionId transaction_id,
                           TTabletId tablet_id, TabletUid tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);

    std::shared_lock txn_rdlock(_get_txn_map_lock(transaction_id));

    auto& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        return;
    }

    auto& tablet_txn_info_map = it->second;
    auto tablet_txn_info_iter = tablet_txn_info_map.find(tablet_info);
    if (tablet_txn_info_iter == tablet_txn_info_map.end()) {
        return;
    }

    auto& txn_info = tablet_txn_info_iter->second;
    txn_info->abort();
}

// delete the txn from manager if it is not committed(not have a valid rowset)
Status TxnManager::rollback_txn(TPartitionId partition_id, const Tablet& tablet,
                                TTransactionId transaction_id) {
    return rollback_txn(partition_id, transaction_id, tablet.tablet_id(), tablet.tablet_uid());
}

Status TxnManager::delete_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                              TTransactionId transaction_id) {
    return delete_txn(tablet->data_dir()->get_meta(), partition_id, transaction_id,
                      tablet->tablet_id(), tablet->tablet_uid());
}

void TxnManager::set_txn_related_delete_bitmap(
        TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
        TabletUid tablet_uid, bool unique_key_merge_on_write, DeleteBitmapPtr delete_bitmap,
        const RowsetIdUnorderedSet& rowset_ids,
        std::shared_ptr<PartialUpdateInfo> partial_update_info) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);

    std::lock_guard<std::shared_mutex> txn_lock(_get_txn_lock(transaction_id));
    {
        // get tx
        std::lock_guard<std::shared_mutex> wrlock(_get_txn_map_lock(transaction_id));
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
        auto it = txn_tablet_map.find(key);
        if (it == txn_tablet_map.end()) {
            LOG(WARNING) << "transaction_id: " << transaction_id
                         << " partition_id: " << partition_id << " may be cleared";
            return;
        }
        auto load_itr = it->second.find(tablet_info);
        if (load_itr == it->second.end()) {
            LOG(WARNING) << "transaction_id: " << transaction_id
                         << " partition_id: " << partition_id << " tablet_id: " << tablet_id
                         << " may be cleared";
            return;
        }
        auto& load_info = load_itr->second;
        load_info->unique_key_merge_on_write = unique_key_merge_on_write;
        load_info->delete_bitmap = delete_bitmap;
        load_info->rowset_ids = rowset_ids;
        load_info->partial_update_info = partial_update_info;
    }
}

Status TxnManager::commit_txn(OlapMeta* meta, TPartitionId partition_id,
                              TTransactionId transaction_id, TTabletId tablet_id,
                              TabletUid tablet_uid, const PUniqueId& load_id,
                              const RowsetSharedPtr& rowset_ptr, PendingRowsetGuard guard,
                              bool is_recovery) {
    if (partition_id < 1 || transaction_id < 1 || tablet_id < 1) {
        LOG(WARNING) << "invalid commit req "
                     << " partition_id=" << partition_id << " transaction_id=" << transaction_id
                     << " tablet_id=" << tablet_id;
        return Status::InternalError("invalid partition id");
    }

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);
    if (rowset_ptr == nullptr) {
        return Status::Error<ROWSET_INVALID>(
                "could not commit txn because rowset ptr is null. partition_id: {}, "
                "transaction_id: {}, tablet: {}",
                key.first, key.second, tablet_info.to_string());
    }

    DBUG_EXECUTE_IF("TxnManager.commit_txn.random_failed", {
        if (rand() % 100 < (100 * dp->param("percent", 0.5))) {
            LOG_WARNING("TxnManager.commit_txn.random_failed")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id);
            return Status::InternalError("debug commit txn random failed");
        }
    });
    DBUG_EXECUTE_IF("TxnManager.commit_txn.wait", {
        if (auto wait = dp->param<int>("duration", 0); wait > 0) {
            LOG_WARNING("TxnManager.commit_txn.wait")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id)
                    .tag("wait ms", wait);
            std::this_thread::sleep_for(std::chrono::milliseconds(wait));
        }
    });

    std::lock_guard<std::shared_mutex> txn_lock(_get_txn_lock(transaction_id));
    // this while loop just run only once, just for if break
    do {
        // get tx
        std::shared_lock rdlock(_get_txn_map_lock(transaction_id));
        auto rs_pb = rowset_ptr->rowset_meta()->get_rowset_pb();
        // TODO(dx): remove log after fix partition id eq 0 bug
        if (!rs_pb.has_partition_id() || rs_pb.partition_id() == 0) {
            rowset_ptr->rowset_meta()->set_partition_id(partition_id);
            LOG(WARNING) << "cant get partition id from rs pb, get from func arg partition_id="
                         << partition_id;
        }
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
        auto it = txn_tablet_map.find(key);
        if (it == txn_tablet_map.end()) {
            break;
        }

        auto load_itr = it->second.find(tablet_info);
        if (load_itr == it->second.end()) {
            break;
        }

        // found load for txn,tablet
        // case 1: user commit rowset, then the load id must be equal
        auto& load_info = load_itr->second;
        // check if load id is equal
        if (load_info->rowset == nullptr) {
            break;
        }

        if (load_info->load_id.hi() != load_id.hi() || load_info->load_id.lo() != load_id.lo()) {
            break;
        }

        // find a rowset with same rowset id, then it means a duplicate call
        if (load_info->rowset->rowset_id() == rowset_ptr->rowset_id()) {
            LOG(INFO) << "find rowset exists when commit transaction to engine."
                      << "partition_id: " << key.first << ", transaction_id: " << key.second
                      << ", tablet: " << tablet_info.to_string()
                      << ", rowset_id: " << load_info->rowset->rowset_id();
            // Should not remove this rowset from pending rowsets
            load_info->pending_rs_guard = std::move(guard);
            return Status::OK();
        }

        // find a rowset with different rowset id, then it should not happen, just return errors
        return Status::Error<PUSH_TRANSACTION_ALREADY_EXIST>(
                "find rowset exists when commit transaction to engine. but rowset ids are not "
                "same. partition_id: {}, transaction_id: {}, tablet: {}, exist rowset_id: {}, new "
                "rowset_id: {}",
                key.first, key.second, tablet_info.to_string(),
                load_info->rowset->rowset_id().to_string(), rowset_ptr->rowset_id().to_string());
    } while (false);

    // if not in recovery mode, then should persist the meta to meta env
    // save meta need access disk, it maybe very slow, so that it is not in global txn lock
    // it is under a single txn lock
    if (!is_recovery) {
        Status save_status =
                RowsetMetaManager::save(meta, tablet_uid, rowset_ptr->rowset_id(),
                                        rowset_ptr->rowset_meta()->get_rowset_pb(), false);
        DBUG_EXECUTE_IF("TxnManager.RowsetMetaManager.save_wait", {
            if (auto wait = dp->param<int>("duration", 0); wait > 0) {
                LOG_WARNING("TxnManager.RowsetMetaManager.save_wait")
                        .tag("txn_id", transaction_id)
                        .tag("tablet_id", tablet_id)
                        .tag("wait ms", wait);
                std::this_thread::sleep_for(std::chrono::milliseconds(wait));
            }
        });
        if (!save_status.ok()) {
            save_status.append(fmt::format(", txn id: {}", transaction_id));
            return save_status;
        }
    }

    {
        std::lock_guard<std::shared_mutex> wrlock(_get_txn_map_lock(transaction_id));
        auto load_info = std::make_shared<TabletTxnInfo>(load_id, rowset_ptr);
        load_info->pending_rs_guard = std::move(guard);
        if (is_recovery) {
            TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(tablet_info.tablet_id,
                                                                          tablet_info.tablet_uid);
            if (tablet != nullptr && tablet->enable_unique_key_merge_on_write()) {
                load_info->unique_key_merge_on_write = true;
                load_info->delete_bitmap.reset(new DeleteBitmap(tablet->tablet_id()));
            }
        }
        load_info->commit();

        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
        txn_tablet_map[key][tablet_info] = std::move(load_info);
        _insert_txn_partition_map_unlocked(transaction_id, partition_id);
        VLOG_NOTICE << "commit transaction to engine successfully."
                    << " partition_id: " << key.first << ", transaction_id: " << key.second
                    << ", tablet: " << tablet_info.to_string()
                    << ", rowsetid: " << rowset_ptr->rowset_id()
                    << ", version: " << rowset_ptr->version().first;
    }
    return Status::OK();
}

// remove a txn from txn manager
Status TxnManager::publish_txn(OlapMeta* meta, TPartitionId partition_id,
                               TTransactionId transaction_id, TTabletId tablet_id,
                               TabletUid tablet_uid, const Version& version,
                               TabletPublishStatistics* stats) {
    auto tablet = _engine.tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        return Status::OK();
    }
    DCHECK(stats != nullptr);

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);
    RowsetSharedPtr rowset;
    std::shared_ptr<TabletTxnInfo> tablet_txn_info;
    int64_t t1 = MonotonicMicros();
    /// Step 1: get rowset, tablet_txn_info by key
    {
        std::shared_lock txn_rlock(_get_txn_lock(transaction_id));
        std::shared_lock txn_map_rlock(_get_txn_map_lock(transaction_id));
        stats->lock_wait_time_us += MonotonicMicros() - t1;

        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
        if (auto it = txn_tablet_map.find(key); it != txn_tablet_map.end()) {
            auto& tablet_map = it->second;
            if (auto txn_info_iter = tablet_map.find(tablet_info);
                txn_info_iter != tablet_map.end()) {
                // found load for txn,tablet
                // case 1: user commit rowset, then the load id must be equal
                tablet_txn_info = txn_info_iter->second;
                rowset = tablet_txn_info->rowset;
            }
        }
    }
    if (rowset == nullptr) {
        return Status::Error<TRANSACTION_NOT_EXIST>(
                "publish txn failed, rowset not found. partition_id={}, transaction_id={}, "
                "tablet={}",
                partition_id, transaction_id, tablet_info.to_string());
    }
    DBUG_EXECUTE_IF("TxnManager.publish_txn.random_failed_before_save_rs_meta", {
        if (rand() % 100 < (100 * dp->param("percent", 0.5))) {
            LOG_WARNING("TxnManager.publish_txn.random_failed_before_save_rs_meta")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id);
            return Status::InternalError("debug publish txn before save rs meta random failed");
        }
    });
    DBUG_EXECUTE_IF("TxnManager.publish_txn.wait_before_save_rs_meta", {
        if (auto wait = dp->param<int>("duration", 0); wait > 0) {
            LOG_WARNING("TxnManager.publish_txn.wait_before_save_rs_meta")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id)
                    .tag("wait ms", wait);
            std::this_thread::sleep_for(std::chrono::milliseconds(wait));
        }
    });

    /// Step 2: make rowset visible
    // save meta need access disk, it maybe very slow, so that it is not in global txn lock
    // it is under a single txn lock
    // TODO(ygl): rowset is already set version here, memory is changed, if save failed
    // it maybe a fatal error
    rowset->make_visible(version);

    DBUG_EXECUTE_IF("TxnManager.publish_txn.random_failed_after_save_rs_meta", {
        if (rand() % 100 < (100 * dp->param("percent", 0.5))) {
            LOG_WARNING("TxnManager.publish_txn.random_failed_after_save_rs_meta")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id);
            return Status::InternalError("debug publish txn after save rs meta random failed");
        }
    });
    DBUG_EXECUTE_IF("TxnManager.publish_txn.wait_after_save_rs_meta", {
        if (auto wait = dp->param<int>("duration", 0); wait > 0) {
            LOG_WARNING("TxnManager.publish_txn.wait_after_save_rs_meta")
                    .tag("txn_id", transaction_id)
                    .tag("tablet_id", tablet_id)
                    .tag("wait ms", wait);
            std::this_thread::sleep_for(std::chrono::milliseconds(wait));
        }
    });
    // update delete_bitmap
    if (tablet_txn_info->unique_key_merge_on_write) {
        std::unique_ptr<RowsetWriter> rowset_writer;
        RETURN_IF_ERROR(tablet->create_transient_rowset_writer(
                rowset, &rowset_writer, tablet_txn_info->partial_update_info));

        int64_t t2 = MonotonicMicros();
        RETURN_IF_ERROR(tablet->update_delete_bitmap(rowset, tablet_txn_info->rowset_ids,
                                                     tablet_txn_info->delete_bitmap, transaction_id,
                                                     rowset_writer.get()));
        int64_t t3 = MonotonicMicros();
        stats->calc_delete_bitmap_time_us = t3 - t2;
        if (tablet_txn_info->partial_update_info &&
            tablet_txn_info->partial_update_info->is_partial_update) {
            // build rowset writer and merge transient rowset
            RETURN_IF_ERROR(rowset_writer->flush());
            RowsetSharedPtr transient_rowset;
            RETURN_IF_ERROR(rowset_writer->build(transient_rowset));
            rowset->merge_rowset_meta(transient_rowset->rowset_meta());

            // erase segment cache cause we will add a segment to rowset
            SegmentLoader::instance()->erase_segments(rowset->rowset_id(), rowset->num_segments());
        }
        stats->partial_update_write_segment_us = MonotonicMicros() - t3;
        int64_t t4 = MonotonicMicros();
        RETURN_IF_ERROR(TabletMetaManager::save_delete_bitmap(
                tablet->data_dir(), tablet->tablet_id(), tablet_txn_info->delete_bitmap,
                version.second));
        stats->save_meta_time_us = MonotonicMicros() - t4;
    }

    /// Step 3:  add to binlog
    auto enable_binlog = tablet->is_enable_binlog();
    if (enable_binlog) {
        auto status = rowset->add_to_binlog();
        if (!status.ok()) {
            return Status::Error<ROWSET_ADD_TO_BINLOG_FAILED>(
                    "add rowset to binlog failed. when publish txn rowset_id: {}, tablet id: {}, "
                    "txn id: {}",
                    rowset->rowset_id().to_string(), tablet_id, transaction_id);
        }
    }

    /// Step 4: save meta
    int64_t t5 = MonotonicMicros();
    auto status = RowsetMetaManager::save(meta, tablet_uid, rowset->rowset_id(),
                                          rowset->rowset_meta()->get_rowset_pb(), enable_binlog);
    stats->save_meta_time_us += MonotonicMicros() - t5;
    if (!status.ok()) {
        status.append(fmt::format(", txn id: {}", transaction_id));
        return status;
    }

    // TODO(Drogon): remove these test codes
    if (enable_binlog) {
        auto version_str = fmt::format("{}", version.first);
        VLOG_DEBUG << fmt::format("tabletid: {}, version: {}, binlog filepath: {}", tablet_id,
                                  version_str, tablet->get_binlog_filepath(version_str));
    }

    /// Step 5: remove tablet_info from tnx_tablet_map
    // txn_tablet_map[key] empty, remove key from txn_tablet_map
    int64_t t6 = MonotonicMicros();
    std::lock_guard<std::shared_mutex> txn_lock(_get_txn_lock(transaction_id));
    std::lock_guard<std::shared_mutex> wrlock(_get_txn_map_lock(transaction_id));
    stats->lock_wait_time_us += MonotonicMicros() - t6;
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    if (auto it = txn_tablet_map.find(key); it != txn_tablet_map.end()) {
        it->second.erase(tablet_info);
        VLOG_NOTICE << "publish txn successfully."
                    << " partition_id: " << key.first << ", txn_id: " << key.second
                    << ", tablet_id: " << tablet_info.tablet_id
                    << ", rowsetid: " << rowset->rowset_id() << ", version: " << version.first
                    << "," << version.second;
        if (it->second.empty()) {
            txn_tablet_map.erase(it);
            _clear_txn_partition_map_unlocked(transaction_id, partition_id);
        }
    }

    return status;
}

// txn could be rollbacked if it does not have related rowset
// if the txn has related rowset then could not rollback it, because it
// may be committed in another thread and our current thread meets errors when writing to data file
// BE has to wait for fe call clear txn api
Status TxnManager::rollback_txn(TPartitionId partition_id, TTransactionId transaction_id,
                                TTabletId tablet_id, TabletUid tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);

    std::lock_guard<std::shared_mutex> wrlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);

    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        return Status::OK();
    }

    auto& tablet_txn_info_map = it->second;
    if (auto load_itr = tablet_txn_info_map.find(tablet_info);
        load_itr != tablet_txn_info_map.end()) {
        // found load for txn,tablet
        // case 1: user commit rowset, then the load id must be equal
        const auto& load_info = load_itr->second;
        if (load_info->rowset != nullptr) {
            return Status::Error<TRANSACTION_ALREADY_COMMITTED>(
                    "if rowset is not null, it means other thread may commit the rowset should "
                    "not delete txn any more");
        }
    }

    tablet_txn_info_map.erase(tablet_info);
    LOG(INFO) << "rollback transaction from engine successfully."
              << " partition_id: " << key.first << ", transaction_id: " << key.second
              << ", tablet: " << tablet_info.to_string();
    if (tablet_txn_info_map.empty()) {
        txn_tablet_map.erase(it);
        _clear_txn_partition_map_unlocked(transaction_id, partition_id);
    }
    return Status::OK();
}

// fe call this api to clear unused rowsets in be
// could not delete the rowset if it already has a valid version
Status TxnManager::delete_txn(OlapMeta* meta, TPartitionId partition_id,
                              TTransactionId transaction_id, TTabletId tablet_id,
                              TabletUid tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);
    std::lock_guard<std::shared_mutex> txn_wrlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        return Status::Error<TRANSACTION_NOT_EXIST>("key not founded from txn_tablet_map");
    }
    auto load_itr = it->second.find(tablet_info);
    if (load_itr != it->second.end()) {
        // found load for txn,tablet
        // case 1: user commit rowset, then the load id must be equal
        auto& load_info = load_itr->second;
        auto& rowset = load_info->rowset;
        if (rowset != nullptr && meta != nullptr) {
            if (rowset->version().first > 0) {
                return Status::Error<TRANSACTION_ALREADY_COMMITTED>(
                        "could not delete transaction from engine, just remove it from memory not "
                        "delete from disk, because related rowset already published. partition_id: "
                        "{}, transaction_id: {}, tablet: {}, rowset id: {}, version:{}",
                        key.first, key.second, tablet_info.to_string(),
                        rowset->rowset_id().to_string(), rowset->version().to_string());
            } else {
                static_cast<void>(RowsetMetaManager::remove(meta, tablet_uid, rowset->rowset_id()));
#ifndef BE_TEST
                _engine.add_unused_rowset(rowset);
#endif
                VLOG_NOTICE << "delete transaction from engine successfully."
                            << " partition_id: " << key.first << ", transaction_id: " << key.second
                            << ", tablet: " << tablet_info.to_string() << ", rowset: "
                            << (rowset != nullptr ? rowset->rowset_id().to_string() : "0");
            }
        }
        it->second.erase(load_itr);
    }
    if (it->second.empty()) {
        txn_tablet_map.erase(it);
        _clear_txn_partition_map_unlocked(transaction_id, partition_id);
    }
    return Status::OK();
}

void TxnManager::get_tablet_related_txns(TTabletId tablet_id, TabletUid tablet_uid,
                                         int64_t* partition_id,
                                         std::set<int64_t>* transaction_ids) {
    if (partition_id == nullptr || transaction_ids == nullptr) {
        LOG(WARNING) << "parameter is null when get transactions by tablet";
        return;
    }

    TabletInfo tablet_info(tablet_id, tablet_uid);
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        txn_tablet_map_t& txn_tablet_map = _txn_tablet_maps[i];
        for (auto& it : txn_tablet_map) {
            if (it.second.find(tablet_info) != it.second.end()) {
                *partition_id = it.first.first;
                transaction_ids->insert(it.first.second);
                VLOG_NOTICE << "find transaction on tablet."
                            << "partition_id: " << it.first.first
                            << ", transaction_id: " << it.first.second
                            << ", tablet: " << tablet_info.to_string();
            }
        }
    }
}

// force drop all txns related with the tablet
// maybe lock error, because not get txn lock before remove from meta
void TxnManager::force_rollback_tablet_related_txns(OlapMeta* meta, TTabletId tablet_id,
                                                    TabletUid tablet_uid) {
    TabletInfo tablet_info(tablet_id, tablet_uid);
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::lock_guard<std::shared_mutex> txn_wrlock(_txn_map_locks[i]);
        txn_tablet_map_t& txn_tablet_map = _txn_tablet_maps[i];
        for (auto it = txn_tablet_map.begin(); it != txn_tablet_map.end();) {
            auto load_itr = it->second.find(tablet_info);
            if (load_itr != it->second.end()) {
                auto& load_info = load_itr->second;
                auto& rowset = load_info->rowset;
                if (rowset != nullptr && meta != nullptr) {
                    LOG(INFO) << " delete transaction from engine "
                              << ", tablet: " << tablet_info.to_string()
                              << ", rowset id: " << rowset->rowset_id();
                    static_cast<void>(
                            RowsetMetaManager::remove(meta, tablet_uid, rowset->rowset_id()));
                }
                LOG(INFO) << "remove tablet related txn."
                          << " partition_id: " << it->first.first
                          << ", transaction_id: " << it->first.second
                          << ", tablet: " << tablet_info.to_string() << ", rowset: "
                          << (rowset != nullptr ? rowset->rowset_id().to_string() : "0");
                it->second.erase(load_itr);
            }
            if (it->second.empty()) {
                _clear_txn_partition_map_unlocked(it->first.second, it->first.first);
                it = txn_tablet_map.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void TxnManager::get_txn_related_tablets(const TTransactionId transaction_id,
                                         TPartitionId partition_id,
                                         std::map<TabletInfo, RowsetSharedPtr>* tablet_infos) {
    // get tablets in this transaction
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    std::shared_lock txn_rdlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        VLOG_NOTICE << "could not find tablet for"
                    << " partition_id=" << partition_id << ", transaction_id=" << transaction_id;
        return;
    }
    auto& load_info_map = it->second;

    // each tablet
    for (auto& load_info : load_info_map) {
        const TabletInfo& tablet_info = load_info.first;
        // must not check rowset == null here, because if rowset == null
        // publish version should failed
        tablet_infos->emplace(tablet_info, load_info.second->rowset);
    }
}

void TxnManager::get_all_related_tablets(std::set<TabletInfo>* tablet_infos) {
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        for (auto& it : _txn_tablet_maps[i]) {
            for (auto& tablet_load_it : it.second) {
                tablet_infos->emplace(tablet_load_it.first);
            }
        }
    }
}

void TxnManager::get_all_commit_tablet_txn_info_by_tablet(
        const TabletSharedPtr& tablet, CommitTabletTxnInfoVec* commit_tablet_txn_info_vec) {
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        for (const auto& [txn_key, load_info_map] : _txn_tablet_maps[i]) {
            auto tablet_load_it = load_info_map.find(tablet->get_tablet_info());
            if (tablet_load_it != load_info_map.end()) {
                const auto& [_, load_info] = *tablet_load_it;
                const auto& rowset = load_info->rowset;
                const auto& delete_bitmap = load_info->delete_bitmap;
                if (!rowset || !delete_bitmap) {
                    continue;
                }
                commit_tablet_txn_info_vec->push_back({
                        .transaction_id = txn_key.second,
                        .partition_id = txn_key.first,
                        .delete_bitmap = delete_bitmap,
                        .rowset_ids = load_info->rowset_ids,
                        .partial_update_info = load_info->partial_update_info,
                });
            }
        }
    }
}

void TxnManager::build_expire_txn_map(std::map<TabletInfo, std::vector<int64_t>>* expire_txn_map) {
    int64_t now = UnixSeconds();
    // traverse the txn map, and get all expired txns
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        for (auto&& [txn_key, tablet_txn_infos] : _txn_tablet_maps[i]) {
            auto txn_id = txn_key.second;
            for (auto&& [tablet_info, txn_info] : tablet_txn_infos) {
                double diff = difftime(now, txn_info->creation_time);
                if (diff < config::pending_data_expire_time_sec) {
                    continue;
                }

                (*expire_txn_map)[tablet_info].push_back(txn_id);
                if (VLOG_IS_ON(3)) {
                    VLOG_NOTICE << "find expired txn."
                                << " tablet=" << tablet_info.to_string()
                                << " transaction_id=" << txn_id << " exist_sec=" << diff;
                }
            }
        }
    }
}

void TxnManager::get_partition_ids(const TTransactionId transaction_id,
                                   std::vector<TPartitionId>* partition_ids) {
    std::shared_lock txn_rdlock(_get_txn_map_lock(transaction_id));
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    auto it = txn_partition_map.find(transaction_id);
    if (it != txn_partition_map.end()) {
        for (int64_t partition_id : it->second) {
            partition_ids->push_back(partition_id);
        }
    }
}

void TxnManager::_insert_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id) {
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    auto find = txn_partition_map.find(transaction_id);
    if (find == txn_partition_map.end()) {
        txn_partition_map[transaction_id] = std::unordered_set<int64_t>();
    }
    txn_partition_map[transaction_id].insert(partition_id);
}

void TxnManager::_clear_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id) {
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    auto it = txn_partition_map.find(transaction_id);
    if (it != txn_partition_map.end()) {
        it->second.erase(partition_id);
        if (it->second.empty()) {
            txn_partition_map.erase(it);
        }
    }
}

void TxnManager::add_txn_tablet_delta_writer(int64_t transaction_id, int64_t tablet_id,
                                             DeltaWriter* delta_writer) {
    std::lock_guard<std::shared_mutex> txn_wrlock(
            _get_txn_tablet_delta_writer_map_lock(transaction_id));
    txn_tablet_delta_writer_map_t& txn_tablet_delta_writer_map =
            _get_txn_tablet_delta_writer_map(transaction_id);
    auto find = txn_tablet_delta_writer_map.find(transaction_id);
    if (find == txn_tablet_delta_writer_map.end()) {
        txn_tablet_delta_writer_map[transaction_id] = std::map<int64_t, DeltaWriter*>();
    }
    txn_tablet_delta_writer_map[transaction_id][tablet_id] = delta_writer;
}

void TxnManager::finish_slave_tablet_pull_rowset(int64_t transaction_id, int64_t tablet_id,
                                                 int64_t node_id, bool is_succeed) {
    std::lock_guard<std::shared_mutex> txn_wrlock(
            _get_txn_tablet_delta_writer_map_lock(transaction_id));
    txn_tablet_delta_writer_map_t& txn_tablet_delta_writer_map =
            _get_txn_tablet_delta_writer_map(transaction_id);
    auto find_txn = txn_tablet_delta_writer_map.find(transaction_id);
    if (find_txn == txn_tablet_delta_writer_map.end()) {
        LOG(WARNING) << "delta writer manager is not exist, txn_id=" << transaction_id
                     << ", tablet_id=" << tablet_id;
        return;
    }
    auto find_tablet = txn_tablet_delta_writer_map[transaction_id].find(tablet_id);
    if (find_tablet == txn_tablet_delta_writer_map[transaction_id].end()) {
        LOG(WARNING) << "delta writer is not exist, txn_id=" << transaction_id
                     << ", tablet_id=" << tablet_id;
        return;
    }
    DeltaWriter* delta_writer = txn_tablet_delta_writer_map[transaction_id][tablet_id];
    delta_writer->finish_slave_tablet_pull_rowset(node_id, is_succeed);
}

void TxnManager::clear_txn_tablet_delta_writer(int64_t transaction_id) {
    std::lock_guard<std::shared_mutex> txn_wrlock(
            _get_txn_tablet_delta_writer_map_lock(transaction_id));
    txn_tablet_delta_writer_map_t& txn_tablet_delta_writer_map =
            _get_txn_tablet_delta_writer_map(transaction_id);
    auto it = txn_tablet_delta_writer_map.find(transaction_id);
    if (it != txn_tablet_delta_writer_map.end()) {
        txn_tablet_delta_writer_map.erase(it);
    }
    VLOG_CRITICAL << "remove delta writer manager, txn_id=" << transaction_id;
}

int64_t TxnManager::get_txn_by_tablet_version(int64_t tablet_id, int64_t version) {
    char key[16];
    memcpy(key, &tablet_id, sizeof(int64_t));
    memcpy(key + sizeof(int64_t), &version, sizeof(int64_t));
    CacheKey cache_key((const char*)&key, sizeof(key));

    auto* handle = _tablet_version_cache->cache()->lookup(cache_key);
    if (handle == nullptr) {
        return -1;
    }
    int64_t res = *(int64_t*)_tablet_version_cache->cache()->value(handle);
    _tablet_version_cache->cache()->release(handle);
    return res;
}

void TxnManager::update_tablet_version_txn(int64_t tablet_id, int64_t version, int64_t txn_id) {
    char key[16];
    memcpy(key, &tablet_id, sizeof(int64_t));
    memcpy(key + sizeof(int64_t), &version, sizeof(int64_t));
    CacheKey cache_key((const char*)&key, sizeof(key));

    int64_t* value = new int64_t;
    *value = txn_id;
    auto deleter = [](const doris::CacheKey& key, void* value) {
        int64_t* cache_value = (int64_t*)value;
        delete cache_value;
    };

    auto* handle = _tablet_version_cache->cache()->insert(cache_key, value, 1, deleter,
                                                          CachePriority::NORMAL, sizeof(txn_id));
    _tablet_version_cache->cache()->release(handle);
}

TxnState TxnManager::get_txn_state(TPartitionId partition_id, TTransactionId transaction_id,
                                   TTabletId tablet_id, TabletUid tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, tablet_uid);

    std::shared_lock txn_rdlock(_get_txn_map_lock(transaction_id));

    auto& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        return TxnState::NOT_FOUND;
    }

    auto& tablet_txn_info_map = it->second;
    auto tablet_txn_info_iter = tablet_txn_info_map.find(tablet_info);
    if (tablet_txn_info_iter == tablet_txn_info_map.end()) {
        return TxnState::NOT_FOUND;
    }

    const auto& txn_info = tablet_txn_info_iter->second;
    return txn_info->state;
}

} // namespace doris
