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

#include "meta-store/keys.h"

#include <set>
#include <string>

#include "meta-store/codec.h"

namespace doris::cloud {

// clang-format off
// Prefix
static const char* INSTANCE_KEY_PREFIX = "instance";

static const char* TXN_KEY_PREFIX      = "txn";
static const char* VERSION_KEY_PREFIX  = "version";
static const char* META_KEY_PREFIX     = "meta";
static const char* RECYCLE_KEY_PREFIX  = "recycle";
static const char* STATS_KEY_PREFIX    = "stats";
static const char* JOB_KEY_PREFIX      = "job";
static const char* COPY_KEY_PREFIX     = "copy";
static const char* VAULT_KEY_PREFIX    = "storage_vault";

// Infix
static const char* TXN_KEY_INFIX_LABEL                  = "txn_label";
static const char* TXN_KEY_INFIX_INFO                   = "txn_info";
static const char* TXN_KEY_INFIX_INDEX                  = "txn_index";
static const char* TXN_KEY_INFIX_RUNNING                = "txn_running";

static const char* PARTITION_VERSION_KEY_INFIX          = "partition";
static const char* TABLE_VERSION_KEY_INFIX              = "table";

static const char* META_KEY_INFIX_ROWSET                = "rowset";
static const char* META_KEY_INFIX_ROWSET_TMP            = "rowset_tmp";
static const char* META_KEY_INFIX_TABLET                = "tablet";
static const char* META_KEY_INFIX_TABLET_IDX            = "tablet_index";
static const char* META_KEY_INFIX_SCHEMA                = "schema";
static const char* META_KEY_INFIX_DELETE_BITMAP         = "delete_bitmap";
static const char* META_KEY_INFIX_DELETE_BITMAP_LOCK    = "delete_bitmap_lock";
static const char* META_KEY_INFIX_DELETE_BITMAP_PENDING = "delete_bitmap_pending";
static const char* META_KEY_INFIX_MOW_TABLET_JOB        = "mow_tablet_job";
static const char* META_KEY_INFIX_SCHEMA_DICTIONARY     = "tablet_schema_pb_dict";
static const char* META_KEY_INFIX_PACKED_FILE          = "packed_file";

static const char* RECYCLE_KEY_INFIX_INDEX              = "index";
static const char* RECYCLE_KEY_INFIX_PART               = "partition";
static const char* RECYCLE_KEY_TXN                      = "txn";

static const char* STATS_KEY_INFIX_TABLET               = "tablet";

static const char* JOB_KEY_INFIX_TABLET                 = "tablet";
static const char* JOB_KEY_INFIX_RL_PROGRESS            = "routine_load_progress";
static const char* JOB_KEY_INFIX_STREAMING_JOB          = "streaming_job";
static const char* JOB_KEY_INFIX_RESTORE_TABLET         = "restore_tablet";
static const char* JOB_KEY_INFIX_RESTORE_ROWSET         = "restore_rowset";
static const char* JOB_KEY_INFIX_SNAPSHOT_DATA_MIGRATOR = "snapshot_data_migrator";
static const char* JOB_KEY_INFIX_SNAPSHOT_CHAIN_COMPACTOR = "snapshot_chain_compactor";

static const char* COPY_JOB_KEY_INFIX                   = "job";
static const char* COPY_FILE_KEY_INFIX                  = "loading_file";
static const char* STAGE_KEY_INFIX                      = "stage";
static const char* VAULT_KEY_INFIX                      = "vault";

// Versioned key constants
static const char* INDEX_KEY_PREFIX                     = "index";
static const char* DATA_KEY_PREFIX                      = "data";
static const char* SNAPSHOT_KEY_PREFIX                  = "snapshot";
static const char* LOG_KEY_PREFIX                       = "log";

static const char* PARTITION_INDEX_KEY_INFIX            = "partition";
static const char* PARTITION_INVERTED_INDEX_KEY_INFIX   = "partition_inverted";
static const char* TABLET_INDEX_KEY_INFIX               = "tablet";
static const char* TABLET_INVERTED_INDEX_KEY_INFIX      = "tablet_inverted";
static const char* INDEX_INDEX_KEY_INFIX                = "index";
static const char* INDEX_INVERTED_KEY_INFIX             = "index_inverted";

static const char* TABLET_LOAD_STATS_KEY_INFIX          = "tablet_load";
static const char* TABLET_COMPACT_STATS_KEY_INFIX       = "tablet_compact";

static const char* META_PARTITION_KEY_INFIX             = "partition";
static const char* META_INDEX_KEY_INFIX                 = "index";
static const char* META_ROWSET_LOAD_KEY_INFIX           = "rowset_load";
static const char* META_ROWSET_COMPACT_KEY_INFIX        = "rowset_compact";
static const char* META_ROWSET_REF_COUNT_KEY_INFIX      = "rowset_ref_count";

static const char* SNAPSHOT_FULL_KEY_INFIX              = "full";
static const char* SNAPSHOT_REFERENCE_KEY_INFIX         = "reference";

// clang-format on

// clang-format off
template <typename T, typename U>
constexpr static bool is_one_of() { return std::is_same_v<T, U>; }
/**
 * Checks the first type is one of the given types (type collection)
 * @param T type to check
 * @param U first type in the collection
 * @param R the rest types in the collection
 */
template <typename T, typename U, typename... R>
constexpr static typename std::enable_if_t<0 < sizeof...(R), bool> is_one_of() {
    return ((std::is_same_v<T, U>) || is_one_of<T, R...>());
}

template <typename T, typename U>
constexpr static bool not_all_types_distinct() { return std::is_same_v<T, U>; }
/**
 * Checks if there are 2 types are the same in the given type list
 */
template <typename T, typename U, typename... R>
constexpr static typename std::enable_if_t<0 < sizeof...(R), bool> 
not_all_types_distinct() {
    // The last part of this expr is a `for` loop
    return is_one_of<T, U>() || is_one_of<T, R...>() || not_all_types_distinct<U, R...>();
}

template <typename T, typename... R>
struct check_types {
    static_assert(is_one_of<T, R...>(), "Invalid key type");
    static_assert(!not_all_types_distinct<R...>(), "Type conflict, there are at least 2 types that are identical in the list.");
    static constexpr bool value = is_one_of<T, R...>() && !not_all_types_distinct<R...>();
};
template <typename T, typename... R>
inline constexpr bool check_types_v = check_types<T, R...>::value;

template <typename T>
static void encode_prefix(const T& t, std::string* key) {
    // Input type T must be one of the following, add if needed
    static_assert(check_types_v<T,
        InstanceKeyInfo,
        TxnLabelKeyInfo, TxnInfoKeyInfo, TxnIndexKeyInfo, TxnRunningKeyInfo,
        MetaRowsetKeyInfo, MetaRowsetTmpKeyInfo, MetaTabletKeyInfo, MetaTabletIdxKeyInfo, MetaSchemaKeyInfo,
        MetaDeleteBitmapInfo, MetaDeleteBitmapUpdateLockInfo, MetaPendingDeleteBitmapInfo, PartitionVersionKeyInfo,
        RecycleIndexKeyInfo, RecyclePartKeyInfo, RecycleRowsetKeyInfo, RecycleTxnKeyInfo, RecycleStageKeyInfo,
        StatsTabletKeyInfo, TableVersionKeyInfo, JobRestoreTabletKeyInfo, JobRestoreRowsetKeyInfo,
        JobTabletKeyInfo, JobRecycleKeyInfo, JobSnapshotDataMigratorKeyInfo, JobSnapshotChainCompactorKeyInfo,
        RLJobProgressKeyInfo, StreamingJobKeyInfo,
        CopyJobKeyInfo, CopyFileKeyInfo,  StorageVaultKeyInfo, MetaSchemaPBDictionaryInfo,
        MowTabletJobInfo, PackedFileKeyInfo>);

    key->push_back(CLOUD_USER_KEY_SPACE01);
    // Prefixes for key families
    if        constexpr (std::is_same_v<T, InstanceKeyInfo>) {
        encode_bytes(INSTANCE_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, TxnLabelKeyInfo>
                      || std::is_same_v<T, TxnInfoKeyInfo>
                      || std::is_same_v<T, TxnIndexKeyInfo>
                      || std::is_same_v<T, TxnRunningKeyInfo>) {
        encode_bytes(TXN_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, MetaRowsetKeyInfo>
                      || std::is_same_v<T, MetaRowsetTmpKeyInfo>
                      || std::is_same_v<T, MetaTabletKeyInfo>
                      || std::is_same_v<T, MetaTabletIdxKeyInfo>
                      || std::is_same_v<T, MetaSchemaKeyInfo>
                      || std::is_same_v<T, MetaSchemaPBDictionaryInfo>
                      || std::is_same_v<T, MetaDeleteBitmapInfo>
                      || std::is_same_v<T, MetaDeleteBitmapUpdateLockInfo>
                      || std::is_same_v<T, MetaPendingDeleteBitmapInfo>
                      || std::is_same_v<T, MowTabletJobInfo>
                      || std::is_same_v<T, PackedFileKeyInfo>) {
        encode_bytes(META_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, PartitionVersionKeyInfo>
                      || std::is_same_v<T, TableVersionKeyInfo>) {
        encode_bytes(VERSION_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, RecycleIndexKeyInfo>
                      || std::is_same_v<T, RecyclePartKeyInfo>
                      || std::is_same_v<T, RecycleRowsetKeyInfo>
                      || std::is_same_v<T, RecycleTxnKeyInfo>
                      || std::is_same_v<T, RecycleStageKeyInfo>) {
        encode_bytes(RECYCLE_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, StatsTabletKeyInfo>) {
        encode_bytes(STATS_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, JobTabletKeyInfo>
                      || std::is_same_v<T, JobRecycleKeyInfo>
                      || std::is_same_v<T, JobSnapshotDataMigratorKeyInfo>
                      || std::is_same_v<T, JobSnapshotChainCompactorKeyInfo>
                      || std::is_same_v<T, RLJobProgressKeyInfo>
                      || std::is_same_v<T, StreamingJobKeyInfo>) {
        encode_bytes(JOB_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, CopyJobKeyInfo>
                      || std::is_same_v<T, CopyFileKeyInfo>) {
        encode_bytes(COPY_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, StorageVaultKeyInfo>) {
        encode_bytes(VAULT_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, JobRestoreTabletKeyInfo>
                      || std::is_same_v<T, JobRestoreRowsetKeyInfo>) {
        encode_bytes(JOB_KEY_PREFIX, key);
    } else {
        // This branch mean to be unreachable, add an assert(false) here to
        // prevent missing branch match.
        // Postpone deduction of static_assert by evaluating sizeof(T)
        static_assert(!sizeof(T), "all types must be matched with if constexpr");
    }
    encode_bytes(std::get<0>(t), key); // instance_id
}
// clang-format on

//==============================================================================
// Resource keys
//==============================================================================

void instance_key(const InstanceKeyInfo& in, std::string* out) {
    encode_prefix(in, out); // 0x01 "instance" ${instance_id}
}

//==============================================================================
// Transaction keys
//==============================================================================

std::string txn_key_prefix(std::string_view instance_id) {
    std::string out;
    encode_prefix(TxnIndexKeyInfo {instance_id, 0}, &out);
    return out;
}

void txn_label_key(const TxnLabelKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                 // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_LABEL, out); // "txn_label"
    encode_int64(std::get<1>(in), out);     // db_id
    encode_bytes(std::get<2>(in), out);     // label
}

void txn_info_key(const TxnInfoKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_INFO, out); // "txn_info"
    encode_int64(std::get<1>(in), out);    // db_id
    encode_int64(std::get<2>(in), out);    // txn_id
}

void txn_index_key(const TxnIndexKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                 // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_INDEX, out); // "txn_index"
    encode_int64(std::get<1>(in), out);     // txn_id
}

void txn_running_key(const TxnRunningKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "txn" ${instance_id}
    encode_bytes(TXN_KEY_INFIX_RUNNING, out); // "txn_running"
    encode_int64(std::get<1>(in), out);       // db_id
    encode_int64(std::get<2>(in), out);       // txn_id
}

//==============================================================================
// Version keys
//==============================================================================

std::string version_key_prefix(std::string_view instance_id) {
    std::string out;
    encode_prefix(TableVersionKeyInfo {instance_id, 0, 0}, &out);
    return out;
}

void table_version_key(const TableVersionKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                     // 0x01 "version" ${instance_id}
    encode_bytes(TABLE_VERSION_KEY_INFIX, out); // "table"
    encode_int64(std::get<1>(in), out);         // db_id
    encode_int64(std::get<2>(in), out);         // tbl_id
}

void partition_version_key(const PartitionVersionKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                         // 0x01 "version" ${instance_id}
    encode_bytes(PARTITION_VERSION_KEY_INFIX, out); // "partition"
    encode_int64(std::get<1>(in), out);             // db_id
    encode_int64(std::get<2>(in), out);             // tbl_id
    encode_int64(std::get<3>(in), out);             // partition_id
}

//==============================================================================
// Meta keys
//==============================================================================

std::string meta_key_prefix(std::string_view instance_id) {
    std::string out;
    encode_prefix(MetaTabletIdxKeyInfo {instance_id, 0}, &out);
    return out;
}

void meta_rowset_key(const MetaRowsetKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET, out); // "rowset"
    encode_int64(std::get<1>(in), out);       // tablet_id
    encode_int64(std::get<2>(in), out);       // version
}

void meta_rowset_tmp_key(const MetaRowsetTmpKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET_TMP, out); // "rowset_tmp"
    encode_int64(std::get<1>(in), out);           // txn_id
    encode_int64(std::get<2>(in), out);           // tablet_id
}

void meta_tablet_key(const MetaTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);       // table_id
    encode_int64(std::get<2>(in), out);       // index_id
    encode_int64(std::get<3>(in), out);       // partition_id
    encode_int64(std::get<4>(in), out);       // tablet_id
}

void meta_tablet_idx_key(const MetaTabletIdxKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_TABLET_IDX, out); // "tablet_index"
    encode_int64(std::get<1>(in), out);           // tablet_id
}

void meta_schema_key(const MetaSchemaKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_SCHEMA, out); // "schema"
    encode_int64(std::get<1>(in), out);       // index_id
    encode_int64(std::get<2>(in), out);       // schema_version
}

void meta_delete_bitmap_key(const MetaDeleteBitmapInfo& in, std::string* out) {
    encode_prefix(in, out);                          // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_DELETE_BITMAP, out); // "delete_bitmap"
    encode_int64(std::get<1>(in), out);              // tablet_id
    encode_bytes(std::get<2>(in), out);              // rowset_id
    encode_int64(std::get<3>(in), out);              // version
    encode_int64(std::get<4>(in), out);              // segment_id
}

void meta_delete_bitmap_update_lock_key(const MetaDeleteBitmapUpdateLockInfo& in,
                                        std::string* out) {
    encode_prefix(in, out);                               // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_DELETE_BITMAP_LOCK, out); // "delete_bitmap_lock"
    encode_int64(std::get<1>(in), out);                   // table_id
    encode_int64(std::get<2>(in), out);                   // partition_id
}

void mow_tablet_job_key(const MowTabletJobInfo& in, std::string* out) {
    encode_prefix(in, out);                           // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_MOW_TABLET_JOB, out); // "mow_tablet_job"
    encode_int64(std::get<1>(in), out);               // table_id
    encode_int64(std::get<2>(in), out);               // initiator
}

void packed_file_key(const PackedFileKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                        // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_PACKED_FILE, out); // "packed_file"
    encode_bytes(std::get<1>(in), out);            // packed_file_path
}

void meta_pending_delete_bitmap_key(const MetaPendingDeleteBitmapInfo& in, std::string* out) {
    encode_prefix(in, out);                                  // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_DELETE_BITMAP_PENDING, out); // "delete_bitmap_pending"
    encode_int64(std::get<1>(in), out);                      // table_id
}

void meta_schema_pb_dictionary_key(const MetaSchemaPBDictionaryInfo& in, std::string* out) {
    encode_prefix(in, out);                              // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_SCHEMA_DICTIONARY, out); // "tablet_schema_pb_dict"
    encode_int64(std::get<1>(in), out);                  // index_id
}

//==============================================================================
// Recycle keys
//==============================================================================

std::string recycle_key_prefix(std::string_view instance_id) {
    std::string out;
    encode_prefix(RecycleIndexKeyInfo {instance_id, 0}, &out);
    return out;
}

void recycle_index_key(const RecycleIndexKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                     // 0x01 "recycle" ${instance_id}
    encode_bytes(RECYCLE_KEY_INFIX_INDEX, out); // "index"
    encode_int64(std::get<1>(in), out);         // index_id
}

void recycle_partition_key(const RecyclePartKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                    // 0x01 "recycle" ${instance_id}
    encode_bytes(RECYCLE_KEY_INFIX_PART, out); // "partition"
    encode_int64(std::get<1>(in), out);        // partition_id
}

void recycle_rowset_key(const RecycleRowsetKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "recycle" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET, out); // "rowset"
    encode_int64(std::get<1>(in), out);       // tablet_id
    encode_bytes(std::get<2>(in), out);       // rowset_id
}

void recycle_txn_key(const RecycleTxnKeyInfo& in, std::string* out) {
    encode_prefix(in, out);             // 0x01 "recycle" ${instance_id}
    encode_bytes(RECYCLE_KEY_TXN, out); // "txn"
    encode_int64(std::get<1>(in), out); // db_id
    encode_int64(std::get<2>(in), out); // txn_id
}

void recycle_stage_key(const RecycleStageKeyInfo& in, std::string* out) {
    encode_prefix(in, out);             // 0x01 "recycle" ${instance_id}
    encode_bytes(STAGE_KEY_INFIX, out); // "stage"
    encode_bytes(std::get<1>(in), out); // stage_id
}

//==============================================================================
// Stats keys
//==============================================================================

void stats_tablet_key(const StatsTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                    // 0x01 "stats" ${instance_id}
    encode_bytes(STATS_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);        // table_id
    encode_int64(std::get<2>(in), out);        // index_id
    encode_int64(std::get<3>(in), out);        // partition_id
    encode_int64(std::get<4>(in), out);        // tablet_id
}

void stats_tablet_data_size_key(const StatsTabletKeyInfo& in, std::string* out) {
    stats_tablet_key(in, out);
    encode_bytes(STATS_KEY_SUFFIX_DATA_SIZE, out);
}
void stats_tablet_num_rows_key(const StatsTabletKeyInfo& in, std::string* out) {
    stats_tablet_key(in, out);
    encode_bytes(STATS_KEY_SUFFIX_NUM_ROWS, out);
}
void stats_tablet_num_rowsets_key(const StatsTabletKeyInfo& in, std::string* out) {
    stats_tablet_key(in, out);
    encode_bytes(STATS_KEY_SUFFIX_NUM_ROWSETS, out);
}
void stats_tablet_num_segs_key(const StatsTabletKeyInfo& in, std::string* out) {
    stats_tablet_key(in, out);
    encode_bytes(STATS_KEY_SUFFIX_NUM_SEGS, out);
}
void stats_tablet_index_size_key(const StatsTabletKeyInfo& in, std::string* out) {
    stats_tablet_key(in, out);
    encode_bytes(STATS_KEY_SUFFIX_INDEX_SIZE, out);
}
void stats_tablet_segment_size_key(const StatsTabletKeyInfo& in, std::string* out) {
    stats_tablet_key(in, out);
    encode_bytes(STATS_KEY_SUFFIX_SEGMENT_SIZE, out);
}

//==============================================================================
// Job keys
//==============================================================================

void job_tablet_key(const JobTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                  // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);      // table_id
    encode_int64(std::get<2>(in), out);      // index_id
    encode_int64(std::get<3>(in), out);      // partition_id
    encode_int64(std::get<4>(in), out);      // tablet_id
}

void job_recycle_key(const JobRecycleKeyInfo& in, std::string* out) {
    encode_prefix(in, out);       // 0x01 "job" ${instance_id}
    encode_bytes("recycle", out); // "recycle"
}

void job_check_key(const JobRecycleKeyInfo& in, std::string* out) {
    encode_prefix(in, out);     // 0x01 "job" ${instance_id}
    encode_bytes("check", out); // "check"
}

void job_snapshot_data_migrator_key(const JobSnapshotDataMigratorKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                                  // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_SNAPSHOT_DATA_MIGRATOR, out); // "snapshot_data_migrator"
}

void job_snapshot_chain_compactor_key(const JobSnapshotChainCompactorKeyInfo& in,
                                      std::string* out) {
    encode_prefix(in, out);                                    // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_SNAPSHOT_CHAIN_COMPACTOR, out); // "snapshot_chain_compactor"
}

void rl_job_progress_key_info(const RLJobProgressKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_RL_PROGRESS, out); // "routine_load_progress"
    encode_int64(std::get<1>(in), out);           // db_id
    encode_int64(std::get<2>(in), out);           // job_id
}

void streaming_job_key(const StreamingJobKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                         // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_STREAMING_JOB, out); // "streaming_job"
    encode_int64(std::get<1>(in), out);             // db_id
    encode_int64(std::get<2>(in), out);             // job_id
}

void job_restore_tablet_key(const JobRestoreTabletKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                          // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_RESTORE_TABLET, out); // "restore_tablet"
    encode_int64(std::get<1>(in), out);              // tablet_id
}

void job_restore_rowset_key(const JobRestoreRowsetKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                          // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_RESTORE_ROWSET, out); // "restore_rowset"
    encode_int64(std::get<1>(in), out);              // tablet_id
    encode_int64(std::get<2>(in), out);              // version
}

//==============================================================================
// Copy keys
//==============================================================================

std::string copy_key_prefix(std::string_view instance_id) {
    std::string out;
    encode_prefix(CopyJobKeyInfo {instance_id, "", 0, "", 0}, &out);
    return out;
}

void copy_job_key(const CopyJobKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                // 0x01 "copy" ${instance_id}
    encode_bytes(COPY_JOB_KEY_INFIX, out); // "job"
    encode_bytes(std::get<1>(in), out);    // stage_id
    encode_int64(std::get<2>(in), out);    // table_id
    encode_bytes(std::get<3>(in), out);    // copy_id
    encode_int64(std::get<4>(in), out);    // group_id
}

void copy_file_key(const CopyFileKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                 // 0x01 "copy" ${instance_id}
    encode_bytes(COPY_FILE_KEY_INFIX, out); // "loading_file"
    encode_bytes(std::get<1>(in), out);     // stage_id
    encode_int64(std::get<2>(in), out);     // table_id
    encode_bytes(std::get<3>(in), out);     // obj_key
    encode_bytes(std::get<4>(in), out);     // obj_etag
}

//==============================================================================
// Storage Vault keys
//==============================================================================

void storage_vault_key(const StorageVaultKeyInfo& in, std::string* out) {
    encode_prefix(in, out);
    encode_bytes(VAULT_KEY_INFIX, out);
    encode_bytes(std::get<1>(in), out);
}

//==============================================================================
// System keys
//==============================================================================

// 0x02 0:"system"  1:"meta-service"  2:"registry"
std::string system_meta_service_registry_key() {
    std::string ret;
    ret.push_back(CLOUD_SYS_KEY_SPACE02);
    encode_bytes("system", &ret);
    encode_bytes("meta-service", &ret);
    encode_bytes("registry", &ret);
    return ret;
}

// 0x02 0:"system"  1:"meta-service"  2:"arn_info"
std::string system_meta_service_arn_info_key() {
    std::string ret;
    ret.push_back(CLOUD_SYS_KEY_SPACE02);
    encode_bytes("system", &ret);
    encode_bytes("meta-service", &ret);
    encode_bytes("arn_info", &ret);
    return ret;
}

// 0x02 0:"system"  1:"meta-service"  2:"encryption_key_info"
std::string system_meta_service_encryption_key_info_key() {
    std::string ret;
    ret.push_back(CLOUD_SYS_KEY_SPACE02);
    encode_bytes("system", &ret);
    encode_bytes("meta-service", &ret);
    encode_bytes("encryption_key_info", &ret);
    return ret;
}

// 0x02 0:"system"  1:"meta-service"  2:"instance_update"
std::string system_meta_service_instance_update_key() {
    std::string ret;
    ret.push_back(CLOUD_SYS_KEY_SPACE02);
    encode_bytes("system", &ret);
    encode_bytes("meta-service", &ret);
    encode_bytes("instance_update", &ret);
    return ret;
}

//==============================================================================
// Other keys
//==============================================================================

namespace versioned {

std::string version_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(VERSION_KEY_PREFIX, &out); // "version"
    encode_bytes(instance_id, &out);        // instance_id
    return out;
}

std::string index_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(INDEX_INDEX_KEY_INFIX, &out); // "version"
    encode_bytes(instance_id, &out);           // instance_id
    return out;
}

std::string stats_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(STATS_KEY_PREFIX, &out); // "stats"
    encode_bytes(instance_id, &out);      // instance_id
    return out;
}

std::string meta_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, &out); // "meta"
    encode_bytes(instance_id, &out);     // instance_id
    return out;
}

std::string data_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(DATA_KEY_PREFIX, &out); // "data"
    encode_bytes(instance_id, &out);     // instance_id
    return out;
}

std::string log_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(LOG_KEY_PREFIX, &out); // "log"
    encode_bytes(instance_id, &out);    // instance_id
    return out;
}

std::string snapshot_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(SNAPSHOT_KEY_PREFIX, &out); // "snapshot"
    encode_bytes(instance_id, &out);         // instance_id
    return out;
}

//==============================================================================
// Version keys
//==============================================================================
void partition_version_key(const PartitionVersionKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(VERSION_KEY_PREFIX, out);          // "version"
    encode_bytes(std::get<0>(in), out);             // instance_id
    encode_bytes(PARTITION_VERSION_KEY_INFIX, out); // "partition"
    encode_int64(std::get<1>(in), out);             // partition_id
}

void table_version_key(const TableVersionKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(VERSION_KEY_PREFIX, out);      // "version"
    encode_bytes(std::get<0>(in), out);         // instance_id
    encode_bytes(TABLE_VERSION_KEY_INFIX, out); // "table"
    encode_int64(std::get<1>(in), out);         // table_id
}

//==============================================================================
// Index keys
//==============================================================================
void partition_index_key(const PartitionIndexKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(INDEX_KEY_PREFIX, out);          // "index"
    encode_bytes(std::get<0>(in), out);           // instance_id
    encode_bytes(PARTITION_INDEX_KEY_INFIX, out); // "partition"
    encode_int64(std::get<1>(in), out);           // partition_id
}

void partition_inverted_index_key(const PartitionInvertedIndexKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(INDEX_KEY_PREFIX, out);                   // "index"
    encode_bytes(std::get<0>(in), out);                    // instance_id
    encode_bytes(PARTITION_INVERTED_INDEX_KEY_INFIX, out); // "partition_inverted"
    encode_int64(std::get<1>(in), out);                    // db_id
    encode_int64(std::get<2>(in), out);                    // table_id
    encode_int64(std::get<3>(in), out);                    // partition_id
}

void tablet_index_key(const TabletIndexKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(INDEX_KEY_PREFIX, out);       // "index"
    encode_bytes(std::get<0>(in), out);        // instance_id
    encode_bytes(TABLET_INDEX_KEY_INFIX, out); // "tablet"
    encode_int64(std::get<1>(in), out);        // tablet_id
}

void tablet_inverted_index_key(const TabletInvertedIndexKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(INDEX_KEY_PREFIX, out);                // "index"
    encode_bytes(std::get<0>(in), out);                 // instance_id
    encode_bytes(TABLET_INVERTED_INDEX_KEY_INFIX, out); // "tablet_inverted"
    encode_int64(std::get<1>(in), out);                 // db_id
    encode_int64(std::get<2>(in), out);                 // table_id
    encode_int64(std::get<3>(in), out);                 // index_id
    encode_int64(std::get<4>(in), out);                 // partition_id
    encode_int64(std::get<5>(in), out);                 // tablet_id
}

void index_index_key(const IndexIndexKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(INDEX_KEY_PREFIX, out);      // "index"
    encode_bytes(std::get<0>(in), out);       // instance_id
    encode_bytes(INDEX_INDEX_KEY_INFIX, out); // "index"
    encode_int64(std::get<1>(in), out);       // index_id
}

void index_inverted_key(const IndexInvertedKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(INDEX_KEY_PREFIX, out);         // "index"
    encode_bytes(std::get<0>(in), out);          // instance_id
    encode_bytes(INDEX_INVERTED_KEY_INFIX, out); // "index_inverted"
    encode_int64(std::get<1>(in), out);          // db_id
    encode_int64(std::get<2>(in), out);          // table_id
    encode_int64(std::get<3>(in), out);          // index_id
}

//==============================================================================
// Stats keys
//==============================================================================
void tablet_load_stats_key(const TabletLoadStatsKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(STATS_KEY_PREFIX, out);            // "stats"
    encode_bytes(std::get<0>(in), out);             // instance_id
    encode_bytes(TABLET_LOAD_STATS_KEY_INFIX, out); // "tablet_load"
    encode_int64(std::get<1>(in), out);             // tablet_id
}

void tablet_compact_stats_key(const TabletCompactStatsKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(STATS_KEY_PREFIX, out);               // "stats"
    encode_bytes(std::get<0>(in), out);                // instance_id
    encode_bytes(TABLET_COMPACT_STATS_KEY_INFIX, out); // "tablet_compact"
    encode_int64(std::get<1>(in), out);                // tablet_id
}

//==============================================================================
// Meta keys
//==============================================================================
void meta_partition_key(const MetaPartitionKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);          // "meta"
    encode_bytes(std::get<0>(in), out);          // instance_id
    encode_bytes(META_PARTITION_KEY_INFIX, out); // "partition"
    encode_int64(std::get<1>(in), out);          // partition_id
}

void meta_index_key(const MetaIndexKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);      // "meta"
    encode_bytes(std::get<0>(in), out);      // instance_id
    encode_bytes(META_INDEX_KEY_INFIX, out); // "index"
    encode_int64(std::get<1>(in), out);      // index_id
}

void meta_tablet_key(const versioned::MetaTabletKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);       // "meta"
    encode_bytes(std::get<0>(in), out);       // instance_id
    encode_bytes(META_KEY_INFIX_TABLET, out); // "tablet"
    encode_int64(std::get<1>(in), out);       // tablet_id
}

void meta_schema_key(const versioned::MetaSchemaKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);       // "meta"
    encode_bytes(std::get<0>(in), out);       // instance_id
    encode_bytes(META_KEY_INFIX_SCHEMA, out); // "schema"
    encode_int64(std::get<1>(in), out);       // index_id
    encode_int64(std::get<2>(in), out);       // schema_version
}

void meta_rowset_load_key(const MetaRowsetLoadKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);            // "meta"
    encode_bytes(std::get<0>(in), out);            // instance_id
    encode_bytes(META_ROWSET_LOAD_KEY_INFIX, out); // "rowset_load"
    encode_int64(std::get<1>(in), out);            // tablet_id
    encode_int64(std::get<2>(in), out);            // version
}

void meta_rowset_compact_key(const MetaRowsetCompactKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);               // "meta"
    encode_bytes(std::get<0>(in), out);               // instance_id
    encode_bytes(META_ROWSET_COMPACT_KEY_INFIX, out); // "rowset_compact"
    encode_int64(std::get<1>(in), out);               // tablet_id
    encode_int64(std::get<2>(in), out);               // version
}

void meta_delete_bitmap_key(const MetaDeleteBitmapInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);              // "meta"
    encode_bytes(std::get<0>(in), out);              // instance_id
    encode_bytes(META_KEY_INFIX_DELETE_BITMAP, out); // "delete_bitmap"
    encode_int64(std::get<1>(in), out);              // tablet_id
    encode_bytes(std::get<2>(in), out);              // rowset_id
}
//==============================================================================
// Data keys
//==============================================================================
void data_rowset_ref_count_key(const DataRowsetRefCountKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(DATA_KEY_PREFIX, out);                 // "data"
    encode_bytes(std::get<0>(in), out);                 // instance_id
    encode_bytes(META_ROWSET_REF_COUNT_KEY_INFIX, out); // "rowset_ref_count"
    encode_int64(std::get<1>(in), out);                 // tablet_id
    encode_bytes(std::get<2>(in), out);                 // rowset_id
}

void meta_rowset_key(const MetaRowsetKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(META_KEY_PREFIX, out);       // "meta"
    encode_bytes(std::get<0>(in), out);       // instance_id
    encode_bytes(META_KEY_INFIX_ROWSET, out); // "rowset"
    encode_int64(std::get<1>(in), out);       // tablet_id
    encode_bytes(std::get<2>(in), out);       // rowset_id
}

//==============================================================================
// Snapshot keys
//==============================================================================
void snapshot_full_key(const SnapshotFullKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(SNAPSHOT_KEY_PREFIX, out);     // "snapshot"
    encode_bytes(std::get<0>(in), out);         // instance_id
    encode_bytes(SNAPSHOT_FULL_KEY_INFIX, out); // "full"
}

void snapshot_reference_key(const SnapshotReferenceKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(SNAPSHOT_KEY_PREFIX, out);          // "snapshot"
    encode_bytes(std::get<0>(in), out);              // instance_id
    encode_bytes(SNAPSHOT_REFERENCE_KEY_INFIX, out); // "reference"
    encode_versionstamp(std::get<1>(in), out);       // timestamp
    encode_bytes(std::get<2>(in), out);              // ref_instance_id
}

std::string snapshot_reference_key_prefix(std::string_view instance_id, Versionstamp timestamp) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(SNAPSHOT_KEY_PREFIX, &out);          // "snapshot"
    encode_bytes(instance_id, &out);                  // instance_id
    encode_bytes(SNAPSHOT_REFERENCE_KEY_INFIX, &out); // "reference"
    encode_versionstamp(timestamp, &out);             // timestamp
    return out;
}

std::string snapshot_reference_key_prefix(std::string_view instance_id) {
    std::string out;
    out.push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(SNAPSHOT_KEY_PREFIX, &out);          // "snapshot"
    encode_bytes(instance_id, &out);                  // instance_id
    encode_bytes(SNAPSHOT_REFERENCE_KEY_INFIX, &out); // "reference"
    return out;
}

//==============================================================================
// Log keys
//==============================================================================
void log_key(const LogKeyInfo& in, std::string* out) {
    out->push_back(CLOUD_VERSIONED_KEY_SPACE03);
    encode_bytes(LOG_KEY_PREFIX, out);  // "log"
    encode_bytes(std::get<0>(in), out); // instance_id
}

} // namespace versioned

//==============================================================================
// Decode keys
//==============================================================================
int decode_key(std::string_view* in,
               std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>>* out,
               Versionstamp* timestamp) {
    int pos = 0;
    int last_len = static_cast<int>(in->size());
    while (!in->empty()) {
        int ret = 0;
        auto tag = in->at(0);
        if (tag == EncodingTag::BYTES_TAG) {
            std::string str;
            ret = decode_bytes(in, &str);
            if (ret != 0) return ret;
            out->emplace_back(std::move(str), tag, pos);
        } else if (tag == EncodingTag::NEGATIVE_FIXED_INT_TAG ||
                   tag == EncodingTag::POSITIVE_FIXED_INT_TAG) {
            int64_t v;
            ret = decode_int64(in, &v);
            if (ret != 0) return ret;
            out->emplace_back(v, tag, pos);
        } else if (tag == EncodingTag::VERSIONSTAMP_TAG) {
            Versionstamp vs;
            ret = decode_versionstamp(in, &vs);
            if (ret != 0) return ret;
            if (timestamp) {
                *timestamp = vs;
            }
            ret = decode_tailing_versionstamp_end(in);
            if (ret != 0) return ret;
            out->emplace_back(vs.to_string(), tag, pos);
        } else {
            return -1;
        }
        pos += last_len - in->size();
        last_len = in->size();
    }
    return 0;
}
//==================================================================================
// Key Prefix Map
//==================================================================================
std::set<std::string> get_key_prefix_contants() {
    std::set<std::string> key_prefix_set;
    key_prefix_set.insert(INSTANCE_KEY_PREFIX);
    key_prefix_set.insert(TXN_KEY_PREFIX);
    key_prefix_set.insert(VERSION_KEY_PREFIX);
    key_prefix_set.insert(META_KEY_PREFIX);
    key_prefix_set.insert(RECYCLE_KEY_PREFIX);
    key_prefix_set.insert(STATS_KEY_PREFIX);
    key_prefix_set.insert(JOB_KEY_PREFIX);
    key_prefix_set.insert(COPY_KEY_PREFIX);
    key_prefix_set.insert(VAULT_KEY_PREFIX);
    return key_prefix_set;
}

std::vector<std::string> get_single_version_meta_key_prefixs() {
    std::vector<std::string> key_prefix_list;
    for (std::string_view prefix : {"meta", "version", "stats"}) {
        std::string key_prefix;
        key_prefix.push_back(CLOUD_USER_KEY_SPACE01);
        encode_bytes(prefix, &key_prefix);
        key_prefix_list.push_back(std::move(key_prefix));
    }
    return key_prefix_list;
}

namespace versioned {

bool decode_table_version_key(std::string_view* in, int64_t* table_id, Versionstamp* timestamp) {
    // 0x03 "version" ${instance_id} "table" ${table_id} ${timestamp}
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    auto res = decode_key(in, &out, timestamp);
    if (res != 0 || out.size() != 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != VERSION_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != TABLE_VERSION_KEY_INFIX) {
            return false;
        }
        *table_id = std::get<int64_t>(std::get<0>(out[3]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

bool decode_partition_inverted_index_key(std::string_view* in, int64_t* db_id, int64_t* table_id,
                                         int64_t* partition_id) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }

    in->remove_prefix(1);

    // 0x03 "index" ${instance_id} "partition_inverted" ${db_id} ${table_id} ${partition}
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 6) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[2])) != PARTITION_INVERTED_INDEX_KEY_INFIX) {
            return false;
        }
        *db_id = std::get<int64_t>(std::get<0>(out[3]));
        *table_id = std::get<int64_t>(std::get<0>(out[4]));
        *partition_id = std::get<int64_t>(std::get<0>(out[5]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

bool decode_meta_partition_key(std::string_view* in, int64_t* partition_id,
                               Versionstamp* timestamp) {
    // 0x03 "meta" ${instance_id} "partition" ${partition_id} ${timestamp}
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    auto res = decode_key(in, &out, timestamp);
    if (res != 0 || out.size() != 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_PARTITION_KEY_INFIX) {
            return false;
        }
        *partition_id = std::get<int64_t>(std::get<0>(out[3]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

bool decode_meta_index_key(std::string_view* in, int64_t* index_id, Versionstamp* timestamp) {
    // 0x03 "meta" ${instance_id} "index" ${index_id} ${timestamp}
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    auto res = decode_key(in, &out, timestamp);
    if (res != 0 || out.size() != 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_INDEX_KEY_INFIX) {
            return false;
        }
        *index_id = std::get<int64_t>(std::get<0>(out[3]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

bool decode_meta_schema_key(std::string_view* in, int64_t* index_id, int64_t* schema_version) {
    // 0x03 "meta" ${instance_id} "schema" ${index_id} ${schema_version}
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    auto res = decode_key(in, &out);
    if (res != 0 || out.size() < 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_KEY_INFIX_SCHEMA) {
            return false;
        }
        *index_id = std::get<int64_t>(std::get<0>(out[3]));
        *schema_version = std::get<int64_t>(std::get<0>(out[4]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

bool decode_meta_tablet_key(std::string_view* in, int64_t* tablet_id, Versionstamp* timestamp) {
    // 0x03 "meta" ${instance_id} "tablet" ${tablet_id} ${timestamp}
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    auto res = decode_key(in, &out, timestamp);
    if (res != 0 || out.size() < 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_KEY_INFIX_TABLET) {
            return false;
        }
        *tablet_id = std::get<int64_t>(std::get<0>(out[3]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode tablet inverted index key
// Return true if decode successfully, otherwise false
bool decode_tablet_inverted_index_key(std::string_view* in, int64_t* db_id, int64_t* table_id,
                                      int64_t* index_id, int64_t* partition_id,
                                      int64_t* tablet_id) {
    // 0x03 "index" ${instance_id} "tablet_inverted" ${db_id} ${table_id} ${index_id} ${partition} ${tablet}
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    auto res = decode_key(in, &out);
    if (res != 0 || out.size() != 8) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != INDEX_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != TABLET_INVERTED_INDEX_KEY_INFIX) {
            return false;
        }
        *db_id = std::get<int64_t>(std::get<0>(out[3]));
        *table_id = std::get<int64_t>(std::get<0>(out[4]));
        *index_id = std::get<int64_t>(std::get<0>(out[5]));
        *partition_id = std::get<int64_t>(std::get<0>(out[6]));
        *tablet_id = std::get<int64_t>(std::get<0>(out[7]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

bool decode_snapshot_ref_key(std::string_view* in, std::string* instance_id,
                             Versionstamp* timestamp, std::string* ref_instance_id) {
    // Key format: 0x03 + encode_bytes("snapshot") + encode_bytes(instance_id) +
    //             encode_bytes("reference") + encode_versionstamp(timestamp) + encode_bytes(ref_instance_id)

    if (in->empty() || (*in)[0] != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    // Decode "snapshot"
    std::string snapshot_prefix;
    if (decode_bytes(in, &snapshot_prefix) != 0 || snapshot_prefix != SNAPSHOT_KEY_PREFIX) {
        return false;
    }

    // Decode instance_id
    if (instance_id && decode_bytes(in, instance_id) != 0) {
        return false;
    } else if (!instance_id) {
        std::string dummy;
        if (decode_bytes(in, &dummy) != 0) {
            return false;
        }
    }

    // Decode "reference"
    std::string reference_infix;
    if (decode_bytes(in, &reference_infix) != 0 ||
        reference_infix != SNAPSHOT_REFERENCE_KEY_INFIX) {
        return false;
    }

    // Decode versionstamp (10 bytes)
    if (timestamp && decode_versionstamp(in, timestamp) != 0) {
        return false;
    } else if (!timestamp) {
        Versionstamp dummy;
        if (decode_versionstamp(in, &dummy) != 0) {
            return false;
        }
    }

    // Decode ref_instance_id
    if (ref_instance_id && decode_bytes(in, ref_instance_id) != 0) {
        return false;
    }

    return true;
}

bool decode_data_rowset_ref_count_key(std::string_view* in, int64_t* tablet_id,
                                      std::string* rowset_id) {
    // 0x03 "data" ${instance_id} "rowset_ref_count" ${tablet_id} ${rowset_id}
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_VERSIONED_KEY_SPACE03) {
        return false;
    }
    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    auto res = decode_key(in, &out);
    if (res != 0 || out.size() != 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != DATA_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_ROWSET_REF_COUNT_KEY_INFIX) {
            return false;
        }
        *tablet_id = std::get<int64_t>(std::get<0>(out[3]));
        *rowset_id = std::get<std::string>(std::get<0>(out[4]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}
} // namespace versioned

// Decode stats tablet key to extract table_id, index_id, partition_id and tablet_id
// 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id}
bool decode_stats_tablet_key(std::string_view* in, int64_t* table_id, int64_t* index_id,
                             int64_t* partition_id, int64_t* tablet_id) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 7) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != STATS_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != STATS_KEY_INFIX_TABLET) {
            return false;
        }
        *table_id = std::get<int64_t>(std::get<0>(out[3]));
        *index_id = std::get<int64_t>(std::get<0>(out[4]));
        *partition_id = std::get<int64_t>(std::get<0>(out[5]));
        *tablet_id = std::get<int64_t>(std::get<0>(out[6]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode table version key to extract db_id and tbl_id
// 0x01 "version" ${instance_id} "table" ${db_id} ${tbl_id}
bool decode_table_version_key(std::string_view* in, int64_t* db_id, int64_t* tbl_id) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != VERSION_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != TABLE_VERSION_KEY_INFIX) {
            return false;
        }
        *db_id = std::get<int64_t>(std::get<0>(out[3]));
        *tbl_id = std::get<int64_t>(std::get<0>(out[4]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode tablet schema key to extract index_id and schema_version
// 0x01 "meta" ${instance_id} "schema" ${index_id} ${schema_version}
bool decode_tablet_schema_key(std::string_view* in, int64_t* index_id, int64_t* schema_version) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_KEY_INFIX_SCHEMA) {
            return false;
        }
        *index_id = std::get<int64_t>(std::get<0>(out[3]));
        *schema_version = std::get<int64_t>(std::get<0>(out[4]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode partition version key to extract db_id, tbl_id and partition_id
// 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id}
bool decode_partition_version_key(std::string_view* in, int64_t* db_id, int64_t* tbl_id,
                                  int64_t* partition_id) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 6) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != VERSION_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != PARTITION_VERSION_KEY_INFIX) {
            return false;
        }
        *db_id = std::get<int64_t>(std::get<0>(out[3]));
        *tbl_id = std::get<int64_t>(std::get<0>(out[4]));
        *partition_id = std::get<int64_t>(std::get<0>(out[5]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode meta tablet key to extract table_id, index_id, partition_id and tablet_id
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id}
bool decode_meta_tablet_key(std::string_view* in, int64_t* table_id, int64_t* index_id,
                            int64_t* partition_id, int64_t* tablet_id) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 7) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_KEY_INFIX_TABLET) {
            return false;
        }
        *table_id = std::get<int64_t>(std::get<0>(out[3]));
        *index_id = std::get<int64_t>(std::get<0>(out[4]));
        *partition_id = std::get<int64_t>(std::get<0>(out[5]));
        *tablet_id = std::get<int64_t>(std::get<0>(out[6]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode meta rowset key to extract tablet_id and version
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}
bool decode_meta_rowset_key(std::string_view* in, int64_t* tablet_id, int64_t* version) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 5) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_KEY_INFIX_ROWSET) {
            return false;
        }
        *tablet_id = std::get<int64_t>(std::get<0>(out[3]));
        *version = std::get<int64_t>(std::get<0>(out[4]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode meta tablet index key to extract tablet_id
// 0x01 "meta" ${instance_id} "tablet_index" ${tablet_id}
bool decode_meta_tablet_idx_key(std::string_view* in, int64_t* tablet_id) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 4) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != META_KEY_PREFIX ||
            std::get<std::string>(std::get<0>(out[2])) != META_KEY_INFIX_TABLET_IDX) {
            return false;
        }
        *tablet_id = std::get<int64_t>(std::get<0>(out[3]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

// Decode instance key
bool decode_instance_key(std::string_view* in, std::string* instance_id) {
    if (in->empty() || static_cast<uint8_t>((*in)[0]) != CLOUD_USER_KEY_SPACE01) {
        return false;
    }

    in->remove_prefix(1);

    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
    if (decode_key(in, &out) != 0 || out.size() != 2) {
        return false;
    }

    try {
        if (std::get<std::string>(std::get<0>(out[0])) != INSTANCE_KEY_PREFIX) {
            return false;
        }
        *instance_id = std::get<std::string>(std::get<0>(out[1]));
    } catch (const std::bad_variant_access& e) {
        return false;
    }

    return true;
}

} // namespace doris::cloud
