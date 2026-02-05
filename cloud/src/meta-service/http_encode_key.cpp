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

#include <brpc/controller.h>
#include <brpc/uri.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>

#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/doris_txn.h"
#include "meta-service/meta_service_http.h"
#include "meta-service/meta_service_schema.h"
#include "meta-service/meta_service_tablet_stats.h"
#include "meta-store/blob_message.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

namespace doris::cloud {

// Initializes a tuple with give params, bind runtime input params to a tuple
// like structure
// TODO(gavin): add type restriction with static_assert or concept
// clang-format off
template <typename R>
struct KeyInfoSetter {
    // Gets the result with input params
    R get() {
        R r;
        if (params.size() != std::tuple_size_v<typename R::base_type>) { return r; }
        init(r, std::make_index_sequence<std::tuple_size_v<typename R::base_type>>{});
        return r;
    }

    // Iterate over all fields and set with value
    template<typename T, size_t... I>
    void init(T&& t, std::index_sequence<I...>) {
        (set_helper<T, I>(t, params), ...);
    }

    // Set the given element in tuple with index `I`.
    // The trick is using `if constexpr` to choose compilation code statically
    // even though `std::get<I>(t)` is either `string` or `int64_t`.
    template <typename T, size_t I>
    void set_helper(T&& t, const std::vector<const std::string*>& params) {
        using base_type = typename std::remove_reference_t<T>::base_type;
        if constexpr (std::is_same_v<std::tuple_element_t<I, base_type>, std::string>) {
            std::get<I>(t) = *params[I];
        }
        if constexpr (std::is_same_v<std::tuple_element_t<I, base_type>, int64_t>) {
            std::get<I>(t) = std::strtoll(params[I]->c_str(), nullptr, 10);
        }
    }

    std::vector<const std::string*> params;
};
// clang-format on

template <typename Message>
static std::shared_ptr<Message> parse_json(const std::string& json) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Message>);
    auto ret = std::make_shared<Message>();
    auto st = google::protobuf::util::JsonStringToMessage(json, ret.get());
    if (st.ok()) return ret;
    std::string err = "failed to strictly parse json message, error: " + st.ToString();
    LOG_WARNING(err).tag("json", json);
    // ignore unknown fields
    // google::protobuf::util::JsonParseOptions json_parse_options;
    // json_parse_options.ignore_unknown_fields = true;
    // return google::protobuf::util::JsonStringToMessage(body, req, json_parse_options);
    return nullptr;
}

using param_type = const std::vector<const std::string*>;

template <class ProtoType>
static std::string parse(const ValueBuf& buf) {
    ProtoType pb;
    if (!buf.to_pb(&pb)) return "";
    return proto_to_json(pb);
}

template <class ProtoType>
static std::string parse_to_json(const std::string& buf) {
    ProtoType pb;
    if (!pb.ParseFromString(buf)) return "";
    return proto_to_json(pb);
}

static std::string parse_txn_label(const ValueBuf& buf) {
    std::string value;
    if (buf.iters.size() != 1 || buf.iters[0]->size() != 1) {
        // FIXME(plat1ko): Hard code as we are confident that the `TxnLabel` length
        //  will not exceed the splitting threshold
        return value;
    }
    buf.iters[0]->reset();
    auto [k, v] = buf.iters[0]->next();
    if (v.size() < VERSION_STAMP_LEN) {
        return value;
    }
    int64_t txn_id = 0;
    if (0 != get_txn_id_from_fdb_ts(v.substr(v.size() - VERSION_STAMP_LEN, v.size()), &txn_id)) {
        return value;
    }
    TxnLabelPB pb;
    if (!pb.ParseFromArray(v.data(), v.size() - VERSION_STAMP_LEN)) {
        return value;
    }
    value = proto_to_json(pb);
    value += fmt::format("\ntxn_id={}", txn_id);
    return value;
}

static std::string parse_delete_bitmap(const ValueBuf& buf) {
    std::stringstream ss;
    ss << std::hex << std::setw(2) << std::setfill('0');
    for (auto&& it : buf.iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, v] = it->next();
            for (auto i : v) ss << ((int16_t)i & 0xff);
        }
    }
    return ss.str();
}

static std::string parse_tablet_schema(const ValueBuf& buf) {
    doris::TabletSchemaCloudPB pb;
    if (!parse_schema_value(buf, &pb)) return "";
    return proto_to_json(pb);
}

static std::string parse_tablet_stats(const ValueBuf& buf) {
    if (buf.iters.empty()) {
        return "stats_kvs not found\n";
    }

    std::vector<std::pair<std::string, std::string>> stats_kvs;
    stats_kvs.reserve(5);
    for (auto& i : buf.iters) {
        while (i->has_next()) {
            auto [k, v] = i->next();
            stats_kvs.emplace_back(std::string {k.data(), k.size()},
                                   std::string {v.data(), v.size()});
        }
    }

    if (stats_kvs.empty()) {
        return "stats_kvs not found\n";
    }

    TabletStatsPB stats;
    auto [k, v] = stats_kvs[0];
    stats.ParseFromArray(v.data(), v.size());

    std::string json;
    json += "aggregated_stats: " + proto_to_json(stats) + "\n";

    // Parse split tablet stats
    TabletStats detached_stats;
    int ret = get_detached_tablet_stats(stats_kvs, detached_stats);
    if (ret != 0) {
        json += "failed to get detached_stats, ret=" + std::to_string(ret) + "\n";
        return json;
    }
    TabletStatsPB detached_stats_pb;
    merge_tablet_stats(detached_stats_pb, detached_stats); // convert to pb
    json += "detached_stats: " + proto_to_json(detached_stats_pb) + "\n";

    merge_tablet_stats(stats, detached_stats);

    json += "merged_stats: " + proto_to_json(stats) + "\n";
    return json;
}

static std::string parse_empty_value(const std::string& buf) {
    return "";
}

static std::string parse_int64_value(const std::string& buf) {
    if (buf.size() != sizeof(int64_t)) {
        return "";
    }
    int64_t value = 0;
    std::memcpy(&value, buf.data(), sizeof(int64_t));
    return std::to_string(value);
}

// Helper function to parse versionstamp from URI query parameter
static std::optional<Versionstamp> parse_versionstamp_from_uri(const brpc::URI& uri) {
    std::string_view vs_str = http_query(uri, "versionstamp");
    if (vs_str.empty()) {
        return std::nullopt;
    }

    // versionstamp can be in hex format or direct bytes
    std::string vs_bytes = unhex(vs_str);
    if (vs_bytes.size() != 10) {
        LOG(WARNING) << "Invalid versionstamp size: " << vs_bytes.size();
        return std::nullopt;
    }

    std::array<uint8_t, 10> vs_array;
    std::memcpy(vs_array.data(), vs_bytes.data(), 10);
    return Versionstamp(vs_array);
}

// See keys.h to get all types of key, e.g: MetaRowsetKeyInfo
// key_type -> {{param_name1, param_name2 ...}, key_encoding_func, serialized_pb_to_json_parsing_func, json_to_proto_parsing_func}
// where param names are the input for key_encoding_func
// clang-format off
//                        key_type
static std::unordered_map<std::string_view,
                                     // params                      key_encoding_func                        serialized_pb_to_json_parsing_func           json_to_proto_parsing_func
                          std::tuple<std::vector<std::string_view>, std::function<std::string(param_type&)>, std::function<std::string(const ValueBuf&)>, std::function<std::shared_ptr<google::protobuf::Message>(const std::string&)>>> param_set {
    {"InstanceKey",                {{"instance_id"},                                                 [](param_type& p) { return instance_key(KeyInfoSetter<InstanceKeyInfo>{p}.get());                                      }, parse<InstanceInfoPB>           , parse_json<InstanceInfoPB>}},
    {"TxnLabelKey",                {{"instance_id", "db_id", "label"},                               [](param_type& p) { return txn_label_key(KeyInfoSetter<TxnLabelKeyInfo>{p}.get());                                     }, parse_txn_label                 , parse_json<TxnLabelPB>}},
    {"TxnInfoKey",                 {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return txn_info_key(KeyInfoSetter<TxnInfoKeyInfo>{p}.get());                                       }, parse<TxnInfoPB>                , parse_json<TxnInfoPB>}},
    {"TxnIndexKey",                {{"instance_id", "txn_id"},                                       [](param_type& p) { return txn_index_key(KeyInfoSetter<TxnIndexKeyInfo>{p}.get());                                     }, parse<TxnIndexPB>               , parse_json<TxnIndexPB>}},
    {"TxnRunningKey",              {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return txn_running_key(KeyInfoSetter<TxnRunningKeyInfo>{p}.get());                                 }, parse<TxnRunningPB>             , parse_json<TxnRunningPB>}},
    {"PartitionVersionKey",        {{"instance_id", "db_id", "tbl_id", "partition_id"},              [](param_type& p) { return partition_version_key(KeyInfoSetter<PartitionVersionKeyInfo>{p}.get());                     }, parse<VersionPB>                , parse_json<VersionPB>}},
    {"TableVersionKey",            {{"instance_id", "db_id", "tbl_id"},                              [](param_type& p) { return table_version_key(KeyInfoSetter<TableVersionKeyInfo>{p}.get());                             }, parse<VersionPB>                , parse_json<VersionPB>}},
    {"MetaRowsetKey",              {{"instance_id", "tablet_id", "version"},                         [](param_type& p) { return meta_rowset_key(KeyInfoSetter<MetaRowsetKeyInfo>{p}.get());                                 }, parse<doris::RowsetMetaCloudPB> , parse_json<doris::RowsetMetaCloudPB>}},
    {"MetaRowsetTmpKey",           {{"instance_id", "txn_id", "tablet_id"},                          [](param_type& p) { return meta_rowset_tmp_key(KeyInfoSetter<MetaRowsetTmpKeyInfo>{p}.get());                          }, parse<doris::RowsetMetaCloudPB> , parse_json<doris::RowsetMetaCloudPB>}},
    {"MetaTabletKey",              {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return meta_tablet_key(KeyInfoSetter<MetaTabletKeyInfo>{p}.get());                                 }, parse<doris::TabletMetaCloudPB> , parse_json<doris::TabletMetaCloudPB>}},
    {"MetaTabletIdxKey",           {{"instance_id", "tablet_id"},                                    [](param_type& p) { return meta_tablet_idx_key(KeyInfoSetter<MetaTabletIdxKeyInfo>{p}.get());                          }, parse<TabletIndexPB>            , parse_json<TabletIndexPB>}},
    {"RecycleIndexKey",            {{"instance_id", "index_id"},                                     [](param_type& p) { return recycle_index_key(KeyInfoSetter<RecycleIndexKeyInfo>{p}.get());                             }, parse<RecycleIndexPB>           , parse_json<RecycleIndexPB>}},
    {"RecyclePartKey",             {{"instance_id", "part_id"},                                      [](param_type& p) { return recycle_partition_key(KeyInfoSetter<RecyclePartKeyInfo>{p}.get());                          }, parse<RecyclePartitionPB>       , parse_json<RecyclePartitionPB>}},
    {"RecycleRowsetKey",           {{"instance_id", "tablet_id", "rowset_id"},                       [](param_type& p) { return recycle_rowset_key(KeyInfoSetter<RecycleRowsetKeyInfo>{p}.get());                           }, parse<RecycleRowsetPB>          , parse_json<RecycleRowsetPB>}},
    {"RecycleTxnKey",              {{"instance_id", "db_id", "txn_id"},                              [](param_type& p) { return recycle_txn_key(KeyInfoSetter<RecycleTxnKeyInfo>{p}.get());                                 }, parse<RecycleTxnPB>             , parse_json<RecycleTxnPB>}},
    {"StatsTabletKey",             {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return stats_tablet_key(KeyInfoSetter<StatsTabletKeyInfo>{p}.get());                               }, parse_tablet_stats              , parse_json<TabletStatsPB>}},
    {"JobTabletKey",               {{"instance_id", "table_id", "index_id", "part_id", "tablet_id"}, [](param_type& p) { return job_tablet_key(KeyInfoSetter<JobTabletKeyInfo>{p}.get());                                   }, parse<TabletJobInfoPB>          , parse_json<TabletJobInfoPB>}},
    {"CopyJobKey",                 {{"instance_id", "stage_id", "table_id", "copy_id", "group_id"},  [](param_type& p) { return copy_job_key(KeyInfoSetter<CopyJobKeyInfo>{p}.get());                                       }, parse<CopyJobPB>                , parse_json<CopyJobPB>}},
    {"CopyFileKey",                {{"instance_id", "stage_id", "table_id", "obj_key", "obj_etag"},  [](param_type& p) { return copy_file_key(KeyInfoSetter<CopyFileKeyInfo>{p}.get());                                     }, parse<CopyFilePB>               , parse_json<CopyFilePB>}},
    {"RecycleStageKey",            {{"instance_id", "stage_id"},                                     [](param_type& p) { return recycle_stage_key(KeyInfoSetter<RecycleStageKeyInfo>{p}.get());                             }, parse<RecycleStagePB>           , parse_json<RecycleStagePB>}},
    {"JobRecycleKey",              {{"instance_id"},                                                 [](param_type& p) { return job_check_key(KeyInfoSetter<JobRecycleKeyInfo>{p}.get());                                   }, parse<JobRecyclePB>             , parse_json<JobRecyclePB>}},
    {"MetaSchemaKey",              {{"instance_id", "index_id", "schema_version"},                   [](param_type& p) { return meta_schema_key(KeyInfoSetter<MetaSchemaKeyInfo>{p}.get());                                 }, parse_tablet_schema             , parse_json<doris::TabletSchemaCloudPB>}},
    {"MetaDeleteBitmap",           {{"instance_id", "tablet_id", "rowest_id", "version", "seg_id"},  [](param_type& p) { return meta_delete_bitmap_key(KeyInfoSetter<MetaDeleteBitmapInfo>{p}.get());                       }, parse_delete_bitmap             , parse_json<DeleteBitmapPB>}},
    {"MetaDeleteBitmapUpdateLock", {{"instance_id", "table_id", "partition_id"},                     [](param_type& p) { return meta_delete_bitmap_update_lock_key(KeyInfoSetter<MetaDeleteBitmapUpdateLockInfo>{p}.get()); }, parse<DeleteBitmapUpdateLockPB> , parse_json<DeleteBitmapUpdateLockPB>}},
    {"MetaPendingDeleteBitmap",    {{"instance_id", "tablet_id"},                                    [](param_type& p) { return meta_pending_delete_bitmap_key(KeyInfoSetter<MetaPendingDeleteBitmapInfo>{p}.get());        }, parse<PendingDeleteBitmapPB>    , parse_json<PendingDeleteBitmapPB>}},
    {"RLJobProgressKey",           {{"instance_id", "db_id", "job_id"},                              [](param_type& p) { return rl_job_progress_key_info(KeyInfoSetter<RLJobProgressKeyInfo>{p}.get());                     }, parse<RoutineLoadProgressPB>    , parse_json<RoutineLoadProgressPB>}},
    {"StorageVaultKey",            {{"instance_id", "vault_id"},                                     [](param_type& p) { return storage_vault_key(KeyInfoSetter<StorageVaultKeyInfo>{p}.get());                             }, parse<StorageVaultPB>           , parse_json<StorageVaultPB>}},
    {"MetaSchemaPBDictionaryKey",  {{"instance_id", "index_id"},                                     [](param_type& p) { return meta_schema_pb_dictionary_key(KeyInfoSetter<MetaSchemaPBDictionaryInfo>{p}.get());          }, parse<SchemaCloudDictionary>    , parse_json<SchemaCloudDictionary>}},
    {"MetaServiceRegistryKey",     {std::vector<std::string_view> {},                                [](param_type& p) { return system_meta_service_registry_key();                                                         }, parse<ServiceRegistryPB>        , parse_json<ServiceRegistryPB>}},
    {"MetaServiceArnInfoKey",      {std::vector<std::string_view> {},                                [](param_type& p) { return system_meta_service_arn_info_key();                                                         }, parse<RamUserPB>                , parse_json<RamUserPB>}},
    {"MetaServiceEncryptionKey",   {std::vector<std::string_view> {},                                [](param_type& p) { return system_meta_service_encryption_key_info_key();                                              }, parse<EncryptionKeyInfoPB>      , parse_json<EncryptionKeyInfoPB>}},
    {"PackedFileKey",              {{"instance_id", "packed_file_path"},                             [](param_type& p) { return packed_file_key(KeyInfoSetter<PackedFileKeyInfo>{p}.get());                                 }, parse<PackedFileInfoPB>         , parse_json<PackedFileInfoPB>}},
};

// Versioned key support (0x03 space)
// key_type -> {{param_name1, param_name2 ...}, key_encoding_func, is_versioned_key, parse_func, json_to_proto_func}
static std::unordered_map<std::string_view,
                          std::tuple<std::vector<std::string_view>,
                                     std::function<std::string(param_type&)>,
                                     bool,
                                     std::function<std::string(const std::string&)>,
                                     std::function<std::shared_ptr<google::protobuf::Message>(const std::string&)>>> versioned_param_set {
    {"VersionedPartitionVersionKey",        {{"instance_id", "partition_id"},                                [](param_type& p) { return versioned::partition_version_key(KeyInfoSetter<versioned::PartitionVersionKeyInfo>{p}.get());      }, true, parse_to_json<VersionPB>, parse_json<VersionPB>}},
    {"VersionedTableVersionKey",            {{"instance_id", "table_id"},                                    [](param_type& p) { return versioned::table_version_key(KeyInfoSetter<versioned::TableVersionKeyInfo>{p}.get());              }, true, parse_empty_value, nullptr}},
    {"VersionedPartitionIndexKey",          {{"instance_id", "partition_id"},                                [](param_type& p) { return versioned::partition_index_key(KeyInfoSetter<versioned::PartitionIndexKeyInfo>{p}.get());          }, false, parse_to_json<PartitionIndexPB>, parse_json<PartitionIndexPB>}},
    {"VersionedPartitionInvertedIndexKey",  {{"instance_id", "db_id", "table_id", "partition_id"},          [](param_type& p) { return versioned::partition_inverted_index_key(KeyInfoSetter<versioned::PartitionInvertedIndexKeyInfo>{p}.get()); }, false, parse_empty_value, nullptr}},
    {"VersionedTabletIndexKey",             {{"instance_id", "tablet_id"},                                   [](param_type& p) { return versioned::tablet_index_key(KeyInfoSetter<versioned::TabletIndexKeyInfo>{p}.get());                }, false, parse_to_json<TabletIndexPB>, parse_json<TabletIndexPB>}},
    {"VersionedTabletInvertedIndexKey",     {{"instance_id", "db_id", "table_id", "index_id", "partition_id", "tablet_id"}, [](param_type& p) { return versioned::tablet_inverted_index_key(KeyInfoSetter<versioned::TabletInvertedIndexKeyInfo>{p}.get()); }, false, parse_empty_value, nullptr}},
    {"VersionedIndexIndexKey",              {{"instance_id", "index_id"},                                    [](param_type& p) { return versioned::index_index_key(KeyInfoSetter<versioned::IndexIndexKeyInfo>{p}.get());                  }, false, parse_to_json<IndexIndexPB>, parse_json<IndexIndexPB>}},
    {"VersionedIndexInvertedKey",           {{"instance_id", "db_id", "table_id", "index_id"},               [](param_type& p) { return versioned::index_inverted_key(KeyInfoSetter<versioned::IndexInvertedKeyInfo>{p}.get());            }, false, parse_empty_value, nullptr}},
    {"VersionedTabletLoadStatsKey",         {{"instance_id", "tablet_id"},                                   [](param_type& p) { return versioned::tablet_load_stats_key(KeyInfoSetter<versioned::TabletLoadStatsKeyInfo>{p}.get());        },  true, parse_to_json<TabletStatsPB>, parse_json<TabletStatsPB>}},
    {"VersionedTabletCompactStatsKey",      {{"instance_id", "tablet_id"},                                   [](param_type& p) { return versioned::tablet_compact_stats_key(KeyInfoSetter<versioned::TabletCompactStatsKeyInfo>{p}.get());  }, true, parse_to_json<TabletStatsPB>, parse_json<TabletStatsPB>}},
    {"VersionedMetaPartitionKey",           {{"instance_id", "partition_id"},                                [](param_type& p) { return versioned::meta_partition_key(KeyInfoSetter<versioned::MetaPartitionKeyInfo>{p}.get());            }, true, parse_empty_value, nullptr}},
    {"VersionedMetaIndexKey",               {{"instance_id", "index_id"},                                    [](param_type& p) { return versioned::meta_index_key(KeyInfoSetter<versioned::MetaIndexKeyInfo>{p}.get());                    }, true, parse_empty_value, nullptr}},
    {"VersionedMetaTabletKey",              {{"instance_id", "tablet_id"},                                   [](param_type& p) { return versioned::meta_tablet_key(KeyInfoSetter<versioned::MetaTabletKeyInfo>{p}.get());                  }, true, parse_to_json<doris::TabletMetaCloudPB>, parse_json<doris::TabletMetaCloudPB>}},
    {"VersionedMetaSchemaKey",              {{"instance_id", "index_id", "schema_version"},                  [](param_type& p) { return versioned::meta_schema_key(KeyInfoSetter<versioned::MetaSchemaKeyInfo>{p}.get());                  }, false, parse_to_json<doris::TabletSchemaCloudPB>, parse_json<doris::TabletSchemaCloudPB>}},
    {"VersionedMetaRowsetLoadKey",          {{"instance_id", "tablet_id", "version"},                        [](param_type& p) { return versioned::meta_rowset_load_key(KeyInfoSetter<versioned::MetaRowsetLoadKeyInfo>{p}.get());          }, true, [](const std::string& s) { return ""; }, parse_json<doris::RowsetMetaCloudPB>}},
    {"VersionedMetaRowsetCompactKey",       {{"instance_id", "tablet_id", "version"},                        [](param_type& p) { return versioned::meta_rowset_compact_key(KeyInfoSetter<versioned::MetaRowsetCompactKeyInfo>{p}.get());    }, true, [](const std::string& s) { return ""; }, parse_json<doris::RowsetMetaCloudPB>}},
    {"VersionedMetaDeleteBitmapKey",        {{"instance_id", "tablet_id", "rowset_id"},                      [](param_type& p) { return versioned::meta_delete_bitmap_key(KeyInfoSetter<versioned::MetaDeleteBitmapInfo>{p}.get());         }, false, parse_to_json<DeleteBitmapStoragePB>, parse_json<DeleteBitmapStoragePB>}},
    {"VersionedDataRowsetRefCountKey",      {{"instance_id", "tablet_id", "rowset_id"},                      [](param_type& p) { return versioned::data_rowset_ref_count_key(KeyInfoSetter<versioned::DataRowsetRefCountKeyInfo>{p}.get()); }, false, parse_int64_value, nullptr}},
};
// clang-format on

static MetaServiceResponseStatus encode_key(const brpc::URI& uri, std::string* key,
                                            std::string* key_type = nullptr) {
    MetaServiceResponseStatus status;
    status.set_code(MetaServiceCode::OK);
    std::string_view kt = http_query(uri, "key_type");
    if (key_type != nullptr) *key_type = kt;

    // Try single version key first
    auto it = param_set.find(kt);
    if (it != param_set.end()) {
        auto& key_params = std::get<0>(it->second);
        std::remove_cv_t<param_type> params;
        params.reserve(key_params.size());
        for (auto& i : key_params) {
            auto p = uri.GetQuery(i.data());
            if (p == nullptr || p->empty()) {
                status.set_code(MetaServiceCode::INVALID_ARGUMENT);
                status.set_msg(fmt::format("{} is not given or empty", i));
                return status;
            }
            params.emplace_back(p);
        }
        auto& key_encoding_function = std::get<1>(it->second);
        *key = key_encoding_function(params);
        return status;
    }

    // Try versioned key
    auto vit = versioned_param_set.find(kt);
    if (vit != versioned_param_set.end()) {
        auto& key_params = std::get<0>(vit->second);
        std::remove_cv_t<param_type> params;
        params.reserve(key_params.size());
        for (auto& i : key_params) {
            auto p = uri.GetQuery(i.data());
            if (p == nullptr || p->empty()) {
                status.set_code(MetaServiceCode::INVALID_ARGUMENT);
                status.set_msg(fmt::format("{} is not given or empty", i));
                return status;
            }
            params.emplace_back(p);
        }
        auto& key_encoding_function = std::get<1>(vit->second);
        *key = key_encoding_function(params);
        return status;
    }

    status.set_code(MetaServiceCode::INVALID_ARGUMENT);
    status.set_msg(fmt::format("key_type not supported: {}", (kt.empty() ? "(empty)" : kt)));
    return status;
}

HttpResponse process_http_get_value(TxnKv* txn_kv, const brpc::URI& uri) {
    std::string key;
    std::string key_type;
    if (auto hex_key = http_query(uri, "key"); !hex_key.empty()) {
        key = unhex(hex_key);
        key_type = http_query(uri, "key_type");
    } else { // Encode key from params
        auto st = encode_key(uri, &key, &key_type);
        if (st.code() != MetaServiceCode::OK) {
            return http_json_reply(st);
        }
    }

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
        return http_json_reply(MetaServiceCode::KV_TXN_CREATE_ERR,
                               fmt::format("failed to create txn, err={}", err));
    }

    // Check if it's a versioned key
    auto vit = versioned_param_set.find(key_type);
    if (vit != versioned_param_set.end()) {
        bool is_versioned_key = std::get<2>(vit->second);
        auto& parse_function = std::get<3>(vit->second);

        // Get versionstamp parameter, default to Versionstamp::max() if not provided
        Versionstamp snapshot_version = Versionstamp::max();
        auto vs_opt = parse_versionstamp_from_uri(uri);
        if (vs_opt.has_value()) {
            snapshot_version = *vs_opt;
        }

        std::string value_json;
        Versionstamp actual_version;
        if (is_versioned_key) {
            if (key_type == "VersionedMetaRowsetLoadKey" ||
                key_type == "VersionedMetaRowsetCompactKey") {
                doris::RowsetMetaCloudPB pb;
                err = versioned::document_get(txn.get(), key, snapshot_version, &pb,
                                              &actual_version, false);
                if (err == TxnErrorCode::TXN_OK) {
                    value_json = proto_to_json(pb);
                }
            } else if (key_type == "VersionedMetaSchemaKey") {
                doris::TabletSchemaCloudPB pb;
                err = versioned::document_get(txn.get(), key, snapshot_version, &pb,
                                              &actual_version, false);
                if (err == TxnErrorCode::TXN_OK) {
                    value_json = proto_to_json(pb);
                }
            } else {
                std::string value_str;
                err = versioned_get(txn.get(), key, snapshot_version, &actual_version, &value_str,
                                    false);
                if (err == TxnErrorCode::TXN_OK) {
                    value_json = parse_function(value_str);
                }
            }
            if (!value_json.empty()) {
                value_json += fmt::format("\\nversionstamp={}", actual_version.to_string());
            }
        } else {
            ValueBuf value_buf;
            err = cloud::blob_get(txn.get(), key, &value_buf, true);
            if (err == TxnErrorCode::TXN_OK) {
                value_json = parse_function(value_buf.value());
            }
        }

        if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
            return http_json_reply(MetaServiceCode::KV_TXN_GET_ERR,
                                   fmt::format("versioned key not found, key={}", hex(key)));
        }
        if (err != TxnErrorCode::TXN_OK) {
            return http_json_reply(
                    MetaServiceCode::KV_TXN_GET_ERR,
                    fmt::format("failed to get versioned key, key={}, err={}", hex(key), err));
        }
        if (value_json.empty()) {
            return http_json_reply(
                    MetaServiceCode::PROTOBUF_PARSE_ERR,
                    fmt::format("failed to parse versioned value, key={}", hex(key)));
        }

        return http_text_reply(MetaServiceCode::OK, "", value_json);
    }

    // Handle single version key (original logic)
    auto it = param_set.find(key_type);
    if (it == param_set.end()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               fmt::format("key_type not supported: {}",
                                           (key_type.empty() ? "(empty)" : key_type)));
    }

    ValueBuf value;
    if (key_type == "StatsTabletKey") {
        // FIXME(plat1ko): hard code
        std::string begin_key {key};
        std::string end_key = key + "\xff";
        std::unique_ptr<RangeGetIterator> it;
        bool more = false;
        do {
            err = txn->get(begin_key, end_key, &it, true);
            if (err != TxnErrorCode::TXN_OK) break;
            begin_key = it->next_begin_key();
            more = it->more();
            value.iters.push_back(std::move(it));
        } while (more);
    } else {
        err = cloud::blob_get(txn.get(), key, &value, true);
    }

    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // FIXME: Key not found err
        return http_json_reply(MetaServiceCode::KV_TXN_GET_ERR,
                               fmt::format("kv not found, key={}", hex(key)));
    }
    if (err != TxnErrorCode::TXN_OK) {
        return http_json_reply(MetaServiceCode::KV_TXN_GET_ERR,
                               fmt::format("failed to get kv, key={}", hex(key)));
    }
    auto& value_parsing_function = std::get<2>(it->second);
    auto readable_value = value_parsing_function(value);
    if (readable_value.empty()) [[unlikely]] {
        return http_json_reply(MetaServiceCode::PROTOBUF_PARSE_ERR,
                               fmt::format("failed to parse value, key={}", hex(key)));
    }
    return http_text_reply(MetaServiceCode::OK, "", readable_value);
}

std::string handle_kv_output(std::string_view key, std::string_view value,
                             std::string_view original_value_json, std::string_view new_value_json,
                             std::string_view serialized_value_to_save) {
    std::stringstream final_output;
    final_output << "key_hex=" << hex(key) << "\n"
                 << "original_value_json=" << original_value_json << "\n"
                 << "new_value_json=" << new_value_json << "\n"
                 << "original_value_hex=" << hex(value) << "\n"
                 << "new_value_hex=" << hex(serialized_value_to_save) << "\n";
    std::string final_json_str = final_output.str();
    LOG(INFO) << final_json_str;
    if (final_json_str.size() > 25000) {
        using namespace std::chrono;
        auto ts = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        std::string file_path = fmt::format("/tmp/{}_{}.txt", hex(key), ts);
        LOG(INFO) << "write to file=" << file_path << ", key=" << hex(key)
                  << " size=" << final_json_str.size();
        try {
            std::ofstream kv_file(file_path);
            if (kv_file.is_open()) {
                kv_file << final_json_str;
                kv_file.close();
            }
        } catch (...) {
            LOG(INFO) << "write tmp file failed: " << file_path;
        }
    }

    return final_json_str;
}

HttpResponse process_http_set_value(TxnKv* txn_kv, brpc::Controller* cntl) {
    const brpc::URI& uri = cntl->http_request().uri();
    std::string body = cntl->request_attachment().to_string();
    LOG(INFO) << "set value, body=" << body;

    std::string key;
    std::string key_type;
    if (auto hex_key = http_query(uri, "key"); !hex_key.empty()) {
        key = unhex(hex_key);
        key_type = http_query(uri, "key_type");
    } else { // Encode key from params
        auto st = encode_key(uri, &key, &key_type);
        if (st.code() != MetaServiceCode::OK) {
            return http_json_reply(st);
        }
    }
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) [[unlikely]] {
        return http_json_reply(MetaServiceCode::KV_TXN_CREATE_ERR,
                               fmt::format("failed to create txn, err={}", err));
    }

    // Check if it's a versioned key
    auto vit = versioned_param_set.find(key_type);
    if (vit != versioned_param_set.end()) {
        bool is_versioned_key = std::get<2>(vit->second);
        auto& json_to_proto_function = std::get<4>(vit->second);

        std::shared_ptr<google::protobuf::Message> pb_to_save;
        if (json_to_proto_function) {
            pb_to_save = json_to_proto_function(body);
        }

        // Check if versionstamp parameter is provided
        auto vs_opt = parse_versionstamp_from_uri(uri);
        if (is_versioned_key) {
            if (key_type == "VersionedMetaRowsetLoadKey" ||
                key_type == "VersionedMetaRowsetCompactKey" ||
                key_type == "VersionedMetaSchemaKey") {
                bool success = false;
                if (vs_opt.has_value()) {
                    success = versioned::document_put(txn.get(), key, *vs_opt,
                                                      std::move(*pb_to_save));
                } else {
                    success = versioned::document_put(txn.get(), key, std::move(*pb_to_save));
                }
                if (!success) {
                    return http_json_reply(
                            MetaServiceCode::UNDEFINED_ERR,
                            fmt::format("failed to put versioned document, key={}", hex(key)));
                }
            } else {
                std::string value_to_save;
                if (json_to_proto_function) {
                    value_to_save = pb_to_save->SerializeAsString();
                    if (value_to_save.empty()) {
                        return http_json_reply(
                                MetaServiceCode::UNDEFINED_ERR,
                                fmt::format("failed to serialize pb, key={}", hex(key)));
                    }
                } else {
                    value_to_save = body;
                }

                // Put the value
                if (vs_opt.has_value()) {
                    versioned_put(txn.get(), key, *vs_opt, value_to_save);
                } else {
                    versioned_put(txn.get(), key, value_to_save);
                }
            }
        } else {
            std::string value_to_save;
            if (key_type == "VersionedDataRowsetRefCountKey") {
                // Parse int64 from body
                try {
                    int64_t count = std::stoll(body);
                    value_to_save.resize(sizeof(int64_t));
                    std::memcpy(value_to_save.data(), &count, sizeof(int64_t));
                } catch (...) {
                    return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                                           "failed to parse int64 value from body");
                }
            } else if (json_to_proto_function) {
                value_to_save = pb_to_save->SerializeAsString();
                if (value_to_save.empty()) {
                    return http_json_reply(MetaServiceCode::UNDEFINED_ERR,
                                           fmt::format("failed to serialize pb, key={}", hex(key)));
                }
            } else {
                value_to_save = body;
            }
            txn->put(key, value_to_save);
        }

        LOG(WARNING) << "set_value saved, key=" << hex(key);

        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            return http_json_reply(
                    MetaServiceCode::UNDEFINED_ERR,
                    fmt::format("failed to commit versioned key, key={}, err={}", hex(key), err));
        }

        LOG(WARNING) << "set_value saved versioned key, key=" << hex(key);
        return http_text_reply(MetaServiceCode::OK, "",
                               fmt::format("versioned key saved successfully, key={}", hex(key)));
    }

    auto it = param_set.find(key_type);
    if (it == param_set.end()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               fmt::format("key_type not supported: {}",
                                           (key_type.empty() ? "(empty)" : key_type)));
    }

    auto& json_parsing_function = std::get<3>(it->second);
    std::shared_ptr<google::protobuf::Message> pb_to_save = json_parsing_function(body);
    if (pb_to_save == nullptr) {
        LOG(WARNING) << "invalid input json value for key_type=" << key_type;
        return http_json_reply(
                MetaServiceCode::INVALID_ARGUMENT,
                fmt::format("invalid input json value, cannot parse json to pb, key_type={}",
                            key_type));
    }

    LOG(INFO) << "parsed pb to save key_type=" << key_type << " key=" << hex(key)
              << " pb_to_save=" << proto_to_json(*pb_to_save);

    // ATTN:
    // StatsTabletKey is special, it has a series of keys, we only handle the base stat key
    // MetaSchemaPBDictionaryKey, MetaSchemaKey, MetaDeleteBitmapKey are splited in to multiple KV
    ValueBuf value;
    err = cloud::blob_get(txn.get(), key, &value, true);

    bool kv_found = true;
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        // it is possible the key-value to set is non-existed
        kv_found = false;
    } else if (err != TxnErrorCode::TXN_OK) {
        return http_json_reply(MetaServiceCode::KV_TXN_GET_ERR,
                               fmt::format("failed to get kv, key={}", hex(key)));
    }

    auto concat = [](const std::vector<std::string>& keys) {
        std::stringstream s;
        for (auto& i : keys) s << hex(i) << ", ";
        return s.str();
    };
    LOG(INFO) << "set value, key_type=" << key_type << " " << value.keys().size() << " keys=["
              << concat(value.keys()) << "]";

    std::string original_value_json;
    if (kv_found) {
        auto& serialized_value_to_json_parsing_function = std::get<2>(it->second);
        original_value_json = serialized_value_to_json_parsing_function(value);
        if (original_value_json.empty()) [[unlikely]] {
            LOG(WARNING) << "failed to parse value, key=" << hex(key)
                         << " val=" << hex(value.value());
            return http_json_reply(MetaServiceCode::UNDEFINED_ERR,
                                   fmt::format("failed to parse value, key={}", hex(key)));
        } else {
            LOG(INFO) << "original_value_json=" << original_value_json;
        }
    }
    std::string serialized_value_to_save = pb_to_save->SerializeAsString();
    if (serialized_value_to_save.empty()) {
        LOG(WARNING) << "failed to serialize, key=" << hex(key);
        return http_json_reply(MetaServiceCode::UNDEFINED_ERR,
                               fmt::format("failed to serialize, key={}", hex(key)));
    }
    // we need to remove the original KVs, it may be a range of keys
    // and the number of final keys may be less than the initial number of keys
    if (kv_found) value.remove(txn.get());

    // TODO(gavin): use cloud::blob_put() to deal with split-multi-kv and special keys
    // StatsTabletKey is special, it has a series of keys, we only handle the base stat key
    // MetaSchemaPBDictionaryKey, MetaSchemaKey, MetaDeleteBitmapKey are splited in to multiple KV
    txn->put(key, serialized_value_to_save);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        std::stringstream ss;
        ss << "failed to commit txn when set value, err=" << err << " key=" << hex(key);
        LOG(WARNING) << ss.str();
        return http_json_reply(MetaServiceCode::UNDEFINED_ERR, ss.str());
    }
    LOG(WARNING) << "set_value saved, key=" << hex(key);

    std::string final_json_str =
            handle_kv_output(key, value.value(), original_value_json, proto_to_json(*pb_to_save),
                             serialized_value_to_save);

    return http_text_reply(MetaServiceCode::OK, "", final_json_str);
}

HttpResponse process_http_encode_key(const brpc::URI& uri) {
    std::string key;
    auto st = encode_key(uri, &key);
    if (st.code() != MetaServiceCode::OK) {
        return http_json_reply(st);
    }

    // Append versionstamp to key if versionstamp parameter is provided
    if (auto vs_opt = parse_versionstamp_from_uri(uri); vs_opt.has_value()) {
        key = encode_versioned_key(key, *vs_opt);
    }

    // Print to ensure
    bool unicode = !(uri.GetQuery("unicode") != nullptr && *uri.GetQuery("unicode") == "false");

    auto hex_key = hex(key);
    std::string body = prettify_key(hex_key, unicode);
    TEST_SYNC_POINT_CALLBACK("process_http_encode_key::empty_body", &body);
    if (body.empty()) {
        return http_json_reply(MetaServiceCode::INVALID_ARGUMENT,
                               "failed to decode encoded key, key=" + hex_key,
                               "failed to decode key, it may be malformed");
    }

    static auto format_fdb_key = [](const std::string& s) {
        std::stringstream r;
        for (size_t i = 0; i < s.size(); ++i) {
            if (!(i % 2)) r << "\\x";
            r << s[i];
        }
        return r.str();
    };

    return http_text_reply(MetaServiceCode::OK, "", body + format_fdb_key(hex_key) + "\n");
}

} // namespace doris::cloud
