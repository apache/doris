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

#pragma once

#include <cstdint>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

// clang-format off
// Key encoding schemes:
//
// 0x01 "instance" ${instance_id}                                               -> InstanceInfoPB
//
// 0x01 "txn" ${instance_id} "txn_label" ${db_id} ${label}                      -> TxnLabelPB ${version_timestamp}
// 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${txn_id}                      -> TxnInfoPB
// 0x01 "txn" ${instance_id} "txn_db_tbl" ${txn_id}                             -> TxnIndexPB
// 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${txn_id}                   -> TxnRunningPB
//
// 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id} -> VersionPB
// 0x01 "version" ${instance_id} "table" ${db_id} ${tbl_id}                     -> int64
//
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}                                   -> RowsetMetaCloudPB
// 0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${tablet_id}                                -> RowsetMetaCloudPB
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id}      -> TabletMetaCloudPB
// 0x01 "meta" ${instance_id} "tablet_index" ${tablet_id}                                        -> TabletIndexPB
// 0x01 "meta" ${instance_id} "schema" ${index_id} ${schema_version}                             -> TabletSchemaCloudPB
// 0x01 "meta" ${instance_id} "delete_bitmap_lock" ${table_id} ${partition_id}                   -> DeleteBitmapUpdateLockPB
// 0x01 "meta" ${instance_id} "delete_bitmap_pending" ${table_id}                                -> PendingDeleteBitmapPB 
// 0x01 "meta" ${instance_id} "delete_bitmap" ${tablet_id} ${rowset_id} ${version} ${segment_id} -> roaringbitmap
// 0x01 "meta" ${instance_id} "tablet_schema_pb_dict" ${index_id}                                -> SchemaCloudDictionary
//
// 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id}               -> TabletStatsPB
// 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "data_size"   -> int64
// 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "num_rows"    -> int64
// 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "num_rowsets" -> int64
// 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "num_segs"    -> int64
//
// 0x01 "recycle" ${instance_id} "index" ${index_id}                                       -> RecycleIndexPB
// 0x01 "recycle" ${instance_id} "partition" ${partition_id}                               -> RecyclePartitionPB
// 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id}                        -> RecycleRowsetPB
// 0x01 "recycle" ${instance_id} "txn" ${db_id} ${txn_id}                                  -> RecycleTxnPB
// 0x01 "recycle" ${instance_id} "stage" ${stage_id}                                       -> RecycleStagePB
//
// 0x01 "job" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} -> TabletJobInfoPB
// 0x01 "job" ${instance_id} "recycle"                                                     -> JobRecyclePB
// 0x01 "job" ${instance_id} "check"                                                       -> JobRecyclePB
//
// 0x01 "copy" ${instance_id} "job" ${stage_id} ${table_id} ${copy_id} ${group_id}         -> CopyJobPB
// 0x01 "copy" ${instance_id} "loading_file" ${stage_id} ${table_id} ${obj_name} ${etag}   -> CopyFilePB
//
// 0x01 "storage_vault" ${instance_id} "vault" ${resource_id}                              -> StorageVaultPB
//
// 0x02 "system" "meta-service" "registry"                                                 -> MetaServiceRegistryPB
// 0x02 "system" "meta-service" "arn_info"                                                 -> RamUserPB
// 0x02 "system" "meta-service" "encryption_key_info"                                      -> EncryptionKeyInfoPB
// clang-format on

namespace doris::cloud {

static const constexpr unsigned char CLOUD_USER_KEY_SPACE01 = 0x01;
static const constexpr unsigned char CLOUD_SYS_KEY_SPACE02 = 0x02;
static constexpr uint32_t VERSION_STAMP_LEN = 10;

// Suffix
static constexpr std::string_view STATS_KEY_SUFFIX_DATA_SIZE = "data_size";
static constexpr std::string_view STATS_KEY_SUFFIX_NUM_ROWS = "num_rows";
static constexpr std::string_view STATS_KEY_SUFFIX_NUM_ROWSETS = "num_rowsets";
static constexpr std::string_view STATS_KEY_SUFFIX_NUM_SEGS = "num_segs";

// clang-format off
/**
 * Wraps std::tuple for differnet types even if the underlying type is the same.
 * 
 * @param N for elemination of same underlying types of type alias when we use
 *          `using` to declare a new type.
 *
 * @param Base for base tuple, the underlying type
 */
template<size_t N, typename Base>
struct BasicKeyInfo : Base {
    template<typename... Args>
    BasicKeyInfo(Args&&... args) : Base(std::forward<Args>(args)...) {}
    constexpr static size_t n = N;
    using base_type = Base;
};

// ATTN: newly added key must have different type number

//                                                      0:instance_id
using InstanceKeyInfo      = BasicKeyInfo<0 , std::tuple<std::string>>;

//                                                      0:instance_id  1:db_id  2:label
using TxnLabelKeyInfo      = BasicKeyInfo<1 , std::tuple<std::string,  int64_t, std::string>>;

//                                                      0:instance_id  1:db_id  2:txn_id
using TxnInfoKeyInfo       = BasicKeyInfo<2 , std::tuple<std::string,  int64_t, int64_t>>;

//                                                      0:instance_id  1:txn_id
using TxnIndexKeyInfo      = BasicKeyInfo<3 , std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:db_id  2:txn_id
using TxnRunningKeyInfo    = BasicKeyInfo<5 , std::tuple<std::string,  int64_t, int64_t>>;

//                                                      0:instance_id  1:db_id  2:tbl_id  3:partition_id
using PartitionVersionKeyInfo     = BasicKeyInfo<6 , std::tuple<std::string,  int64_t, int64_t,  int64_t>>;

//                                                      0:instance_id  1:tablet_id  2:version
using MetaRowsetKeyInfo    = BasicKeyInfo<7 , std::tuple<std::string,  int64_t,     int64_t>>;

//                                                      0:instance_id  1:txn_id  2:tablet_id
using MetaRowsetTmpKeyInfo = BasicKeyInfo<8 , std::tuple<std::string,  int64_t,  int64_t>>;

//                                                      0:instance_id  1:table_id  2:index_id  3:part_id  4:tablet_id
using MetaTabletKeyInfo    = BasicKeyInfo<9 , std::tuple<std::string,  int64_t,    int64_t,    int64_t,   int64_t>>;

//                                                      0:instance_id  1:tablet_id
using MetaTabletIdxKeyInfo = BasicKeyInfo<10, std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:index_id
using RecycleIndexKeyInfo  = BasicKeyInfo<11, std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:part_id
using RecyclePartKeyInfo   = BasicKeyInfo<12, std::tuple<std::string,  int64_t>>;

//                                                      0:instance_id  1:tablet_id  2:rowset_id
using RecycleRowsetKeyInfo = BasicKeyInfo<13, std::tuple<std::string,  int64_t,     std::string>>;

//                                                      0:instance_id  1:db_id  2:txn_id
using RecycleTxnKeyInfo    = BasicKeyInfo<14, std::tuple<std::string,  int64_t, int64_t>>;

//                                                      0:instance_id  1:table_id  2:index_id  3:part_id  4:tablet_id
using StatsTabletKeyInfo   = BasicKeyInfo<15, std::tuple<std::string,  int64_t,    int64_t,    int64_t,   int64_t>>;

//                                                      0:instance_id  1:table_id  2:index_id  3:part_id  4:tablet_id
using JobTabletKeyInfo     = BasicKeyInfo<16, std::tuple<std::string,  int64_t,    int64_t,    int64_t,   int64_t>>;

//                                                      0:instance_id  1:stage_id   2:table_id  3:copy_id     4:group_id
using CopyJobKeyInfo       = BasicKeyInfo<17, std::tuple<std::string,  std::string,  int64_t,   std::string,  int64_t>>;

//                                                      0:instance_id  1:stage_id   2:table_id  3:obj_key     4:obj_etag
using CopyFileKeyInfo      = BasicKeyInfo<18, std::tuple<std::string,  std::string,  int64_t,   std::string,  std::string>>;

//                                                      0:instance_id  1:stage_id
using RecycleStageKeyInfo  = BasicKeyInfo<19, std::tuple<std::string,  std::string>>;

//                                                      0:instance_id
using JobRecycleKeyInfo    = BasicKeyInfo<20 , std::tuple<std::string>>;

//                                                      0:instance_id  1:index_id  2:schema_version
using MetaSchemaKeyInfo    = BasicKeyInfo<21, std::tuple<std::string,  int64_t,    int64_t>>;

//                                                      0:instance_id  1:tablet_id  2:rowest_id  3:version  4:seg_id 
using MetaDeleteBitmapInfo = BasicKeyInfo<22 , std::tuple<std::string, int64_t,     std::string, int64_t, int64_t>>;

// partition_id of -1 indicates all partitions
//                                                      0:instance_id  1:table_id 2:partition_id
using MetaDeleteBitmapUpdateLockInfo = BasicKeyInfo<23 , std::tuple<std::string, int64_t, int64_t>>;

//                                                      0:instance_id  1:tablet_id
using MetaPendingDeleteBitmapInfo = BasicKeyInfo<24 , std::tuple<std::string, int64_t>>;

//                                                      0:instance_id 1:db_id  2:job_id
using RLJobProgressKeyInfo = BasicKeyInfo<25, std::tuple<std::string, int64_t, int64_t>>;

//                                                      0:instance_id 1:vault_id
using StorageVaultKeyInfo = BasicKeyInfo<26, std::tuple<std::string, std::string>>;

//                                                      0:instance_id 1:db_id 2:table_id
using TableVersionKeyInfo = BasicKeyInfo<27, std::tuple<std::string, int64_t, int64_t>>;
//                                                      0:instance_id  1:index_id
using MetaSchemaPBDictionaryInfo = BasicKeyInfo<28 , std::tuple<std::string,  int64_t>>;


void instance_key(const InstanceKeyInfo& in, std::string* out);
static inline std::string instance_key(const InstanceKeyInfo& in) { std::string s; instance_key(in, &s); return s; }

void storage_vault_key(const StorageVaultKeyInfo& in, std::string* out);
static inline std::string storage_vault_key(const StorageVaultKeyInfo& in) { std::string s; storage_vault_key(in, &s); return s; }

std::string txn_key_prefix(std::string_view instance_id);
void txn_label_key(const TxnLabelKeyInfo& in, std::string* out);
void txn_info_key(const TxnInfoKeyInfo& in, std::string* out);
void txn_index_key(const TxnIndexKeyInfo& in, std::string* out);
void txn_running_key(const TxnRunningKeyInfo& in, std::string* out);
static inline std::string txn_label_key(const TxnLabelKeyInfo& in) { std::string s; txn_label_key(in, &s); return s; }
static inline std::string txn_info_key(const TxnInfoKeyInfo& in) { std::string s; txn_info_key(in, &s); return s; }
static inline std::string txn_index_key(const TxnIndexKeyInfo& in) { std::string s; txn_index_key(in, &s); return s; }
static inline std::string txn_running_key(const TxnRunningKeyInfo& in) { std::string s; txn_running_key(in, &s); return s; }

std::string version_key_prefix(std::string_view instance_id);
void partition_version_key(const PartitionVersionKeyInfo& in, std::string* out);
static inline std::string partition_version_key(const PartitionVersionKeyInfo& in) { std::string s; partition_version_key(in, &s); return s; }
void table_version_key(const TableVersionKeyInfo& in, std::string* out);
static inline std::string table_version_key(const TableVersionKeyInfo& in) { std::string s; table_version_key(in, &s); return s; }

std::string meta_key_prefix(std::string_view instance_id);
void meta_rowset_key(const MetaRowsetKeyInfo& in, std::string* out);
void meta_rowset_tmp_key(const MetaRowsetTmpKeyInfo& in, std::string* out);
void meta_tablet_idx_key(const MetaTabletIdxKeyInfo& in, std::string* out);
void meta_tablet_key(const MetaTabletKeyInfo& in, std::string* out);
void meta_schema_key(const MetaSchemaKeyInfo& in, std::string* out);
void meta_delete_bitmap_key(const MetaDeleteBitmapInfo& in, std::string* out);
void meta_delete_bitmap_update_lock_key(const MetaDeleteBitmapUpdateLockInfo& in, std::string* out);
void meta_pending_delete_bitmap_key(const MetaPendingDeleteBitmapInfo& in, std::string* out);
void meta_schema_pb_dictionary_key(const MetaSchemaPBDictionaryInfo& in, std::string* out);
static inline std::string meta_rowset_key(const MetaRowsetKeyInfo& in) { std::string s; meta_rowset_key(in, &s); return s; }
static inline std::string meta_rowset_tmp_key(const MetaRowsetTmpKeyInfo& in) { std::string s; meta_rowset_tmp_key(in, &s); return s; }
static inline std::string meta_tablet_idx_key(const MetaTabletIdxKeyInfo& in) { std::string s; meta_tablet_idx_key(in, &s); return s; }
static inline std::string meta_tablet_key(const MetaTabletKeyInfo& in) { std::string s; meta_tablet_key(in, &s); return s; }
static inline std::string meta_schema_key(const MetaSchemaKeyInfo& in) { std::string s; meta_schema_key(in, &s); return s; }
static inline std::string meta_delete_bitmap_key(const MetaDeleteBitmapInfo& in) { std::string s; meta_delete_bitmap_key(in, &s); return s; }
static inline std::string meta_delete_bitmap_update_lock_key(const MetaDeleteBitmapUpdateLockInfo& in) { std::string s; meta_delete_bitmap_update_lock_key(in, &s); return s; }
static inline std::string meta_pending_delete_bitmap_key(const MetaPendingDeleteBitmapInfo& in) { std::string s; meta_pending_delete_bitmap_key(in, &s); return s; }
static inline std::string meta_schema_pb_dictionary_key(const MetaSchemaPBDictionaryInfo& in) { std::string s; meta_schema_pb_dictionary_key(in, &s); return s; }

std::string recycle_key_prefix(std::string_view instance_id);
void recycle_index_key(const RecycleIndexKeyInfo& in, std::string* out);
void recycle_partition_key(const RecyclePartKeyInfo& in, std::string* out);
void recycle_rowset_key(const RecycleRowsetKeyInfo& in, std::string* out);
void recycle_txn_key(const RecycleTxnKeyInfo& in, std::string* out);
void recycle_stage_key(const RecycleStageKeyInfo& in, std::string* out);
static inline std::string recycle_index_key(const RecycleIndexKeyInfo& in) { std::string s; recycle_index_key(in, &s); return s; }
static inline std::string recycle_partition_key(const RecyclePartKeyInfo& in) { std::string s; recycle_partition_key(in, &s); return s; }
static inline std::string recycle_rowset_key(const RecycleRowsetKeyInfo& in) { std::string s; recycle_rowset_key(in, &s); return s; }
static inline std::string recycle_txn_key(const RecycleTxnKeyInfo& in) { std::string s; recycle_txn_key(in, &s); return s; }
static inline std::string recycle_stage_key(const RecycleStageKeyInfo& in) { std::string s; recycle_stage_key(in, &s); return s; }

void stats_tablet_key(const StatsTabletKeyInfo& in, std::string* out);
void stats_tablet_data_size_key(const StatsTabletKeyInfo& in, std::string* out);
void stats_tablet_num_rows_key(const StatsTabletKeyInfo& in, std::string* out);
void stats_tablet_num_rowsets_key(const StatsTabletKeyInfo& in, std::string* out);
void stats_tablet_num_segs_key(const StatsTabletKeyInfo& in, std::string* out);
static inline std::string stats_tablet_key(const StatsTabletKeyInfo& in) { std::string s; stats_tablet_key(in, &s); return s; }

void job_recycle_key(const JobRecycleKeyInfo& in, std::string* out);
void job_check_key(const JobRecycleKeyInfo& in, std::string* out);
static inline std::string job_check_key(const JobRecycleKeyInfo& in) { std::string s; job_check_key(in, &s); return s; }
void job_tablet_key(const JobTabletKeyInfo& in, std::string* out);
static inline std::string job_tablet_key(const JobTabletKeyInfo& in) { std::string s; job_tablet_key(in, &s); return s; }
void rl_job_progress_key_info(const RLJobProgressKeyInfo& in, std::string* out);
static inline std::string rl_job_progress_key_info(const RLJobProgressKeyInfo& in) { std::string s; rl_job_progress_key_info(in, &s); return s; }

std::string copy_key_prefix(std::string_view instance_id);
void copy_job_key(const CopyJobKeyInfo& in, std::string* out);
void copy_file_key(const CopyFileKeyInfo& in, std::string* out);
[[maybe_unused]] static std::string copy_job_key(const CopyJobKeyInfo& in) { std::string s; copy_job_key(in, &s); return s; }
[[maybe_unused]] static std::string copy_file_key(const CopyFileKeyInfo& in) { std::string s; copy_file_key(in, &s); return s; }

std::string system_meta_service_registry_key();
std::string system_meta_service_arn_info_key();

// Note:
// This key points to a value (EncryptionKeyInfoPB, the format is below) which stores a set of items,
// and each item represents a group of encrption key.
// The size of each item: 8 Bytes (int64 key_id) + 32 Bytes * 1.3 (256bit key * base64 amplification factor) = 50 Bytes.
// The maximum size kv of fdb can store: 100k/50Bytes = 2048 items
//
// message EncryptionKeyInfoPB {
//     message Item {
//         optional int64 key_id = 1;
//         optional string key = 2;
//     }
//     repeated Item items = 1;
// }
std::string system_meta_service_encryption_key_info_key();
// clang-format on
// TODO: add a family of decoding functions if needed

/**
 * Decodes a given key without key space byte (the first byte).
 * Note that the input may be partially decode if the return value is non-zero.
 *
 * @param in input byte stream, successfully decoded part will be consumed
 * @param out the vector of each <field decoded, field type and its position> in the input stream
 * @return 0 for successful decoding of the entire input, otherwise error.
 */
int decode_key(std::string_view* in,
               std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>>* out);

} // namespace doris::cloud
