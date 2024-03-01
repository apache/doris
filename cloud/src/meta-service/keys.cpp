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

#include "keys.h"

#include <cassert>
#include <type_traits>
#include <variant>

#include "codec.h"

namespace doris::cloud {

// clang-format off
// Prefix
[[maybe_unused]] static const char* INSTANCE_KEY_PREFIX = "instance";

[[maybe_unused]] static const char* TXN_KEY_PREFIX      = "txn";
[[maybe_unused]] static const char* VERSION_KEY_PREFIX  = "version";
[[maybe_unused]] static const char* META_KEY_PREFIX     = "meta";
[[maybe_unused]] static const char* RECYCLE_KEY_PREFIX  = "recycle";
[[maybe_unused]] static const char* STATS_KEY_PREFIX    = "stats";
[[maybe_unused]] static const char* JOB_KEY_PREFIX      = "job";
[[maybe_unused]] static const char* COPY_KEY_PREFIX     = "copy";

// Infix
[[maybe_unused]] static const char* TXN_KEY_INFIX_LABEL       = "txn_label";
[[maybe_unused]] static const char* TXN_KEY_INFIX_INFO        = "txn_info";
[[maybe_unused]] static const char* TXN_KEY_INFIX_INDEX       = "txn_index";
[[maybe_unused]] static const char* TXN_KEY_INFIX_RUNNING     = "txn_running";

[[maybe_unused]] static const char* VERSION_KEY_INFIX         = "partition";

[[maybe_unused]] static const char* META_KEY_INFIX_ROWSET     = "rowset";
[[maybe_unused]] static const char* META_KEY_INFIX_ROWSET_TMP = "rowset_tmp";
[[maybe_unused]] static const char* META_KEY_INFIX_TABLET     = "tablet";
[[maybe_unused]] static const char* META_KEY_INFIX_TABLET_IDX = "tablet_index";
[[maybe_unused]] static const char* META_KEY_INFIX_SCHEMA     = "schema";
[[maybe_unused]] static const char* META_KEY_INFIX_ROWSET_SCHEMA     = "rowset_schema";
[[maybe_unused]] static const char* META_KEY_INFIX_DELETE_BITMAP = "delete_bitmap";
[[maybe_unused]] static const char* META_KEY_INFIX_DELETE_BITMAP_LOCK = "delete_bitmap_lock";
[[maybe_unused]] static const char* META_KEY_INFIX_DELETE_BITMAP_PENDING = "delete_bitmap_pending";

[[maybe_unused]] static const char* RECYCLE_KEY_INFIX_INDEX   = "index";
[[maybe_unused]] static const char* RECYCLE_KEY_INFIX_PART    = "partition";
[[maybe_unused]] static const char* RECYCLE_KEY_TXN           = "txn";

[[maybe_unused]] static const char* STATS_KEY_INFIX_TABLET    = "tablet";

[[maybe_unused]] static const char* JOB_KEY_INFIX_TABLET      = "tablet";
[[maybe_unused]] static const char* JOB_KEY_INFIX_RL_PROGRESS = "routine_load_progress";

[[maybe_unused]] static const char* COPY_JOB_KEY_INFIX        = "job";
[[maybe_unused]] static const char* COPY_FILE_KEY_INFIX       = "loading_file";
[[maybe_unused]] static const char* STAGE_KEY_INFIX           = "stage";

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
        MetaDeleteBitmapInfo, MetaDeleteBitmapUpdateLockInfo, MetaPendingDeleteBitmapInfo, VersionKeyInfo,
        RecycleIndexKeyInfo, RecyclePartKeyInfo, RecycleRowsetKeyInfo, RecycleTxnKeyInfo, RecycleStageKeyInfo,
        StatsTabletKeyInfo,
        JobTabletKeyInfo, JobRecycleKeyInfo, RLJobProgressKeyInfo,
        CopyJobKeyInfo, CopyFileKeyInfo, MetaRowsetSchemaKeyInfo>);

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
                      || std::is_same_v<T, MetaRowsetSchemaKeyInfo>
                      || std::is_same_v<T, MetaDeleteBitmapInfo>
                      || std::is_same_v<T, MetaDeleteBitmapUpdateLockInfo>
                      || std::is_same_v<T, MetaPendingDeleteBitmapInfo>) {
        encode_bytes(META_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, VersionKeyInfo>) {
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
                      || std::is_same_v<T, RLJobProgressKeyInfo>) {
        encode_bytes(JOB_KEY_PREFIX, key);
    } else if constexpr (std::is_same_v<T, CopyJobKeyInfo>
                      || std::is_same_v<T, CopyFileKeyInfo>) {
        encode_bytes(COPY_KEY_PREFIX, key);
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
    encode_bytes(TXN_KEY_INFIX_LABEL, out); // "txn_index"
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

void version_key(const VersionKeyInfo& in, std::string* out) {
    encode_prefix(in, out);               // 0x01 "version" ${instance_id}
    encode_bytes(VERSION_KEY_INFIX, out); // "partition"
    encode_int64(std::get<1>(in), out);   // db_id
    encode_int64(std::get<2>(in), out);   // tbl_id
    encode_int64(std::get<3>(in), out);   // partition_id
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

void meta_rowset_schema_key(const MetaRowsetSchemaKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                   // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_ROWSET_SCHEMA, out); // "rowset_schema"
    encode_int64(std::get<1>(in), out);       // tablet_id 
    encode_bytes(std::get<2>(in), out);              // rowset_id 
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

void meta_pending_delete_bitmap_key(const MetaPendingDeleteBitmapInfo& in, std::string* out) {
    encode_prefix(in, out);                                  // 0x01 "meta" ${instance_id}
    encode_bytes(META_KEY_INFIX_DELETE_BITMAP_PENDING, out); // "delete_bitmap_pending"
    encode_int64(std::get<1>(in), out);                      // table_id
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

void rl_job_progress_key_info(const RLJobProgressKeyInfo& in, std::string* out) {
    encode_prefix(in, out);                       // 0x01 "job" ${instance_id}
    encode_bytes(JOB_KEY_INFIX_RL_PROGRESS, out); // "routine_load_progress"
    encode_int64(std::get<1>(in), out);           // db_id
    encode_int64(std::get<2>(in), out);           // job_id
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

//==============================================================================
// Other keys
//==============================================================================

//==============================================================================
// Decode keys
//==============================================================================
int decode_key(std::string_view* in,
               std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>>* out) {
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
        } else {
            return -1;
        }
        pos += last_len - in->size();
        last_len = in->size();
    }
    return 0;
}

} // namespace doris::cloud
