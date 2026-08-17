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

#include "meta-service/meta_service_schema.h"

#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <google/protobuf/map.h>
#include <google/protobuf/message_lite.h>
#include <google/protobuf/repeated_field.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/blob_message.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {
namespace config {
extern int16_t meta_schema_value_version;
}

constexpr static const char* VARIANT_TYPE_NAME = "VARIANT";
constexpr static const char* ROW_TTL_HIDDEN_COLUMN_NAME = "__DORIS_TTL_COL__";

struct RowTtlColumnRef {
    bool has_ttl = false;
    int32_t index = -1;
    int32_t unique_id = -1;
};

static bool resolve_row_ttl_column(const doris::TabletSchemaCloudPB& schema,
                                   RowTtlColumnRef* result, std::string* reason) {
    int32_t hidden_index = -1;
    for (int32_t i = 0; i < schema.column_size(); ++i) {
        if (schema.column(i).name() != ROW_TTL_HIDDEN_COLUMN_NAME) {
            continue;
        }
        if (hidden_index != -1) {
            *reason = "tablet schema contains multiple row ttl hidden columns";
            return false;
        }
        hidden_index = i;
    }

    if (!schema.has_ttl_col_idx()) {
        if (hidden_index != -1) {
            result->has_ttl = true;
            result->index = hidden_index;
            result->unique_id = schema.column(hidden_index).unique_id();
        }
        return true;
    }

    const int32_t explicit_index = schema.ttl_col_idx();
    if (explicit_index == -1) {
        if (hidden_index != -1) {
            *reason = "row ttl hidden column exists but ttl_col_idx explicitly disables row ttl";
            return false;
        }
        return true;
    }
    if (explicit_index < -1 || explicit_index >= schema.column_size()) {
        *reason = fmt::format("row ttl column index {} is outside schema with {} columns",
                              explicit_index, schema.column_size());
        return false;
    }
    if (hidden_index == -1) {
        *reason = "ttl_col_idx is set but row ttl hidden column is missing";
        return false;
    }
    if (explicit_index != hidden_index) {
        *reason = fmt::format("ttl_col_idx {} does not point to row ttl hidden column at {}",
                              explicit_index, hidden_index);
        return false;
    }
    result->has_ttl = true;
    result->index = explicit_index;
    result->unique_id = schema.column(explicit_index).unique_id();
    return true;
}

static bool validate_row_ttl_policy(const doris::TabletSchemaCloudPB& schema,
                                    const RowTtlColumnRef& ttl, std::string* reason) {
    if (!ttl.has_ttl) {
        if (schema.has_row_ttl_duration_us() && schema.row_ttl_duration_us() != -1) {
            *reason = "row ttl duration is set without a row ttl hidden column";
            return false;
        }
        if (schema.has_row_ttl_time_zone_offset_seconds()) {
            *reason = "row ttl time zone offset is set without a row ttl hidden column";
            return false;
        }
        return true;
    }
    if (ttl.unique_id < 0) {
        *reason = fmt::format("row ttl hidden column unique id {} is invalid", ttl.unique_id);
        return false;
    }

    const std::string_view ttl_type = schema.column(ttl.index).type();
    if (ttl_type == "BIGINT") {
        if (schema.has_row_ttl_duration_us() && schema.row_ttl_duration_us() != -1) {
            *reason = "direct-expiration row ttl duration must be absent or -1";
            return false;
        }
        if (schema.has_row_ttl_time_zone_offset_seconds()) {
            *reason = "direct-expiration row ttl must not have a time zone offset";
            return false;
        }
        return true;
    }

    const bool temporal_type = ttl_type == "DATE" || ttl_type == "DATEV2" ||
                               ttl_type == "DATETIME" || ttl_type == "DATETIMEV2" ||
                               ttl_type == "TIMESTAMPTZ";
    if (!temporal_type) {
        *reason = fmt::format("unsupported row ttl hidden column type {}", ttl_type);
        return false;
    }
    if (schema.has_row_ttl_duration_us() && schema.row_ttl_duration_us() < -1) {
        *reason = "temporal row ttl duration must be nonnegative or the legacy value -1";
        return false;
    }
    if (ttl_type == "TIMESTAMPTZ") {
        if (schema.has_row_ttl_time_zone_offset_seconds() &&
            schema.row_ttl_time_zone_offset_seconds() != 0) {
            *reason = "TIMESTAMPTZ row ttl time zone offset must be UTC";
            return false;
        }
        return true;
    }

    if (!schema.has_row_ttl_time_zone_offset_seconds()) {
        // Legacy timezone-naive schemas keep an unknown offset. New BEs must preserve that
        // fail-closed state instead of guessing a zone while ordinary compaction continues.
        return true;
    }
    constexpr int32_t MIN_OFFSET_SECONDS = -12 * 60 * 60;
    constexpr int32_t MAX_OFFSET_SECONDS = 14 * 60 * 60;
    const int32_t offset_seconds = schema.row_ttl_time_zone_offset_seconds();
    if (offset_seconds < MIN_OFFSET_SECONDS || offset_seconds > MAX_OFFSET_SECONDS ||
        offset_seconds % 60 != 0) {
        *reason = fmt::format("row ttl time zone offset {} is invalid", offset_seconds);
        return false;
    }
    return true;
}

bool check_row_ttl_policy_compatible(const doris::TabletSchemaCloudPB& schema,
                                     const doris::TabletSchemaCloudPB& saved_schema,
                                     std::string* reason) {
    RowTtlColumnRef incoming_ttl;
    RowTtlColumnRef saved_ttl;
    if (!resolve_row_ttl_column(schema, &incoming_ttl, reason) ||
        !resolve_row_ttl_column(saved_schema, &saved_ttl, reason)) {
        return false;
    }
    if (!validate_row_ttl_policy(schema, incoming_ttl, reason) ||
        !validate_row_ttl_policy(saved_schema, saved_ttl, reason)) {
        return false;
    }
    if (incoming_ttl.has_ttl != saved_ttl.has_ttl) {
        *reason = fmt::format("row ttl presence differs: saved={}, incoming={}", saved_ttl.has_ttl,
                              incoming_ttl.has_ttl);
        return false;
    }
    if (!incoming_ttl.has_ttl) {
        return true;
    }
    if (incoming_ttl.unique_id != saved_ttl.unique_id) {
        *reason = fmt::format("row ttl hidden column unique id differs: saved={}, incoming={}",
                              saved_ttl.unique_id, incoming_ttl.unique_id);
        return false;
    }
    // A new BE materializes an inferred legacy ttl_col_idx and serializes its in-memory unknown
    // duration as -1. These representations are equivalent to absent legacy fields as long as
    // both schemas resolve to the same hidden column and remain in the GC-disabled state.
    const int64_t incoming_duration =
            schema.has_row_ttl_duration_us() ? schema.row_ttl_duration_us() : -1;
    const int64_t saved_duration = saved_schema.has_row_ttl_duration_us()
                                           ? saved_schema.row_ttl_duration_us()
                                           : -1;
    if (incoming_duration != saved_duration) {
        *reason = fmt::format("row ttl duration differs: saved={}, incoming={}",
                              saved_duration, incoming_duration);
        return false;
    }

    const std::string_view ttl_type = schema.column(incoming_ttl.index).type();
    const std::string_view saved_ttl_type = saved_schema.column(saved_ttl.index).type();
    if (ttl_type != saved_ttl_type) {
        *reason = fmt::format("row ttl hidden column type differs: saved={}, incoming={}",
                              saved_ttl_type, ttl_type);
        return false;
    }

    if (ttl_type == "TIMESTAMPTZ") {
        const bool saved_is_utc = !saved_schema.has_row_ttl_time_zone_offset_seconds() ||
                                  saved_schema.row_ttl_time_zone_offset_seconds() == 0;
        const bool incoming_is_utc = !schema.has_row_ttl_time_zone_offset_seconds() ||
                                     schema.row_ttl_time_zone_offset_seconds() == 0;
        if (!saved_is_utc || !incoming_is_utc) {
            *reason = fmt::format("TIMESTAMPTZ row ttl offset must be UTC: saved={}, incoming={}",
                                  saved_schema.has_row_ttl_time_zone_offset_seconds()
                                          ? saved_schema.row_ttl_time_zone_offset_seconds()
                                          : 0,
                                  schema.has_row_ttl_time_zone_offset_seconds()
                                          ? schema.row_ttl_time_zone_offset_seconds()
                                          : 0);
            return false;
        }
        return true;
    }

    if (schema.has_row_ttl_time_zone_offset_seconds() !=
        saved_schema.has_row_ttl_time_zone_offset_seconds()) {
        *reason = "row ttl time zone offset presence differs";
        return false;
    }
    if (schema.has_row_ttl_time_zone_offset_seconds() &&
        schema.row_ttl_time_zone_offset_seconds() !=
                saved_schema.row_ttl_time_zone_offset_seconds()) {
        *reason = fmt::format("row ttl time zone offset differs: saved={}, incoming={}",
                              saved_schema.row_ttl_time_zone_offset_seconds(),
                              schema.row_ttl_time_zone_offset_seconds());
        return false;
    }
    return true;
}

bool check_tablet_schema_compatible(const doris::TabletSchemaCloudPB& schema,
                                    const doris::TabletSchemaCloudPB& saved_schema,
                                    std::string* reason) {
    auto transform = [](std::string_view type) -> std::string_view {
        if (type == "DECIMALV2") return "DECIMAL";
        if (type == "BITMAP") return "OBJECT";
        return type;
    };
    if (saved_schema.column_size() != schema.column_size()) {
        *reason = fmt::format("column count differs: saved={}, incoming={}",
                              saved_schema.column_size(), schema.column_size());
        return false;
    }
    if (!check_row_ttl_policy_compatible(schema, saved_schema, reason)) {
        return false;
    }

    std::vector<const doris::ColumnPB*> saved_columns;
    std::vector<const doris::ColumnPB*> columns;
    saved_columns.reserve(saved_schema.column_size());
    columns.reserve(schema.column_size());
    for (const auto& column : saved_schema.column()) {
        saved_columns.push_back(&column);
    }
    for (const auto& column : schema.column()) {
        columns.push_back(&column);
    }
    auto column_less = [](const doris::ColumnPB* lhs, const doris::ColumnPB* rhs) {
        return lhs->unique_id() < rhs->unique_id();
    };
    std::sort(saved_columns.begin(), saved_columns.end(), column_less);
    std::sort(columns.begin(), columns.end(), column_less);
    for (size_t i = 0; i < saved_columns.size(); ++i) {
        const auto& saved_column = *saved_columns[i];
        const auto& column = *columns[i];
        if (saved_column.unique_id() != column.unique_id() ||
            transform(saved_column.type()) != transform(column.type())) {
            *reason = fmt::format("column differs: saved={}, incoming={}",
                                  saved_column.ShortDebugString(), column.ShortDebugString());
            return false;
        }
    }
    if (saved_schema.index_size() != schema.index_size()) {
        *reason = fmt::format("index count differs: saved={}, incoming={}",
                              saved_schema.index_size(), schema.index_size());
        return false;
    }

    std::vector<const doris::TabletIndexPB*> saved_indexes;
    std::vector<const doris::TabletIndexPB*> indexes;
    saved_indexes.reserve(saved_schema.index_size());
    indexes.reserve(schema.index_size());
    for (const auto& index : saved_schema.index()) {
        saved_indexes.push_back(&index);
    }
    for (const auto& index : schema.index()) {
        indexes.push_back(&index);
    }
    auto index_less = [](const doris::TabletIndexPB* lhs, const doris::TabletIndexPB* rhs) {
        return lhs->index_id() < rhs->index_id();
    };
    std::sort(saved_indexes.begin(), saved_indexes.end(), index_less);
    std::sort(indexes.begin(), indexes.end(), index_less);
    for (size_t i = 0; i < saved_indexes.size(); ++i) {
        const auto& saved_index = *saved_indexes[i];
        const auto& index = *indexes[i];
        if (saved_index.index_id() != index.index_id() ||
            saved_index.index_type() != index.index_type()) {
            *reason = fmt::format("index differs: saved={}, incoming={}",
                                  saved_index.ShortDebugString(), index.ShortDebugString());
            return false;
        }
    }
    return true;
}

doris::TabletSchemaCloudPB normalize_tablet_schema_for_schema_kv(
        const doris::TabletSchemaCloudPB& schema) {
    doris::TabletSchemaCloudPB normalized(schema);

    google::protobuf::RepeatedPtrField<doris::ColumnPB> stable_columns;
    stable_columns.Reserve(normalized.column_size());
    for (const auto& column : normalized.column()) {
        if (column.unique_id() < 0) {
            continue;
        }
        auto* stable_column = stable_columns.Add();
        stable_column->CopyFrom(column);
        stable_column->clear_sparse_columns();
    }
    normalized.mutable_column()->Swap(&stable_columns);

    google::protobuf::RepeatedPtrField<doris::TabletIndexPB> stable_indexes;
    stable_indexes.Reserve(normalized.index_size());
    for (const auto& index : normalized.index()) {
        if (!index.index_suffix_name().empty()) {
            continue;
        }
        stable_indexes.Add()->CopyFrom(index);
    }
    normalized.mutable_index()->Swap(&stable_indexes);
    return normalized;
}

void normalize_tablet_schema_column_types(doris::TabletSchemaCloudPB* schema) {
    for (auto& column : *schema->mutable_column()) {
        if (column.type() == "DECIMAL128") {
            column.mutable_type()->push_back('I');
        }
    }
}

void check_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                     std::string_view schema_key, const doris::TabletSchemaCloudPB& schema) {
    std::string reason;
    if (!check_tablet_schema_compatible(schema, schema, &reason)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("invalid incoming tablet schema, key={}, reason={}", hex(schema_key),
                          reason);
        return;
    }

    TxnErrorCode err = cloud::key_exists(txn, schema_key);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to check existing tablet schema, key={}, err={}", hex(schema_key),
                          err);
        return;
    }

    ValueBuf buf;
    err = cloud::blob_get(txn, schema_key, &buf);
    if (err != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(err);
        msg = fmt::format("failed to read existing tablet schema, key={}, err={}", hex(schema_key),
                          err);
        return;
    }
    doris::TabletSchemaCloudPB saved_schema;
    if (!buf.to_pb(&saved_schema)) {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("failed to parse existing tablet schema, key={}", hex(schema_key));
        return;
    }
    if (!check_tablet_schema_compatible(schema, saved_schema, &reason)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("existing tablet schema is incompatible, key={}, reason={}",
                          hex(schema_key), reason);
        LOG(WARNING) << msg;
    }
}

void check_versioned_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                               std::string_view schema_key,
                               const doris::TabletSchemaCloudPB& schema) {
    std::string reason;
    if (!check_tablet_schema_compatible(schema, schema, &reason)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("invalid incoming versioned tablet schema, key={}, reason={}",
                          hex(schema_key), reason);
        return;
    }

    doris::TabletSchemaCloudPB saved_schema;
    TxnErrorCode err = document_get(txn, schema_key, &saved_schema);
    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return;
    }
    if (err != TxnErrorCode::TXN_OK) {
        msg = fmt::format("failed to read existing versioned tablet schema, key={}, err={}",
                          hex(schema_key), err);
        code = err == TxnErrorCode::TXN_INVALID_DATA ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                                     : cast_as<ErrCategory::READ>(err);
        return;
    }
    if (!check_tablet_schema_compatible(schema, saved_schema, &reason)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("existing versioned tablet schema is incompatible, key={}, reason={}",
                          hex(schema_key), reason);
        LOG(WARNING) << msg;
    }
}

void put_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                   std::string_view schema_key, const doris::TabletSchemaCloudPB& schema) {
    std::string reason;
    if (!check_tablet_schema_compatible(schema, schema, &reason)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("invalid incoming tablet schema, key={}, reason={}", hex(schema_key),
                          reason);
        return;
    }
    TxnErrorCode err = cloud::key_exists(txn, schema_key);
    if (err == TxnErrorCode::TXN_OK) { // schema has already been saved
        TEST_SYNC_POINT_RETURN_WITH_VOID("put_schema_kv:schema_key_exists_return");
        ValueBuf buf;
        err = cloud::blob_get(txn, schema_key, &buf);
        if (err != TxnErrorCode::TXN_OK) {
            code = cast_as<ErrCategory::READ>(err);
            msg = fmt::format("failed to read existing tablet schema, key={}, err={}",
                              hex(schema_key), err);
            return;
        }
        doris::TabletSchemaCloudPB saved_schema;
        if (!buf.to_pb(&saved_schema)) {
            code = MetaServiceCode::PROTOBUF_PARSE_ERR;
            msg = fmt::format("failed to parse existing tablet schema, key={}", hex(schema_key));
            return;
        }
        if (!check_tablet_schema_compatible(schema, saved_schema, &reason)) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format("existing tablet schema is incompatible, key={}, reason={}",
                              hex(schema_key), reason);
            LOG(WARNING) << msg;
        }
        return;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        msg = fmt::format("failed to check that key exists, err={}", err);
        code = cast_as<ErrCategory::READ>(err);
        return;
    }
    LOG_INFO("put schema kv").tag("key", hex(schema_key));
    uint8_t ver = config::meta_schema_value_version;
    if (ver > 0) {
        cloud::blob_put(txn, schema_key, schema, ver);
    } else {
        auto schema_value = schema.SerializeAsString();
        txn->put(schema_key, schema_value);
    }
}

void put_versioned_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                             std::string_view schema_key,
                             const doris::TabletSchemaCloudPB& schema) {
    std::string reason;
    if (!check_tablet_schema_compatible(schema, schema, &reason)) {
        code = MetaServiceCode::INVALID_ARGUMENT;
        msg = fmt::format("invalid incoming versioned tablet schema, key={}, reason={}",
                          hex(schema_key), reason);
        return;
    }
    doris::TabletSchemaCloudPB saved_schema;
    TxnErrorCode err = document_get(txn, schema_key, &saved_schema);
    if (err == TxnErrorCode::TXN_OK) { // schema has already been saved
        TEST_SYNC_POINT_RETURN_WITH_VOID("put_schema_kv:schema_key_exists_return");
        if (!check_tablet_schema_compatible(schema, saved_schema, &reason)) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            msg = fmt::format(
                    "existing versioned tablet schema is incompatible, key={}, "
                    "reason={}",
                    hex(schema_key), reason);
            LOG(WARNING) << msg;
        }
        return;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        msg = fmt::format("failed to read existing versioned tablet schema, key={}, err={}",
                          hex(schema_key), err);
        code = err == TxnErrorCode::TXN_INVALID_DATA ? MetaServiceCode::PROTOBUF_PARSE_ERR
                                                     : cast_as<ErrCategory::READ>(err);
        return;
    }
    LOG_INFO("put versioned schema kv").tag("key", hex(schema_key));
    doris::TabletSchemaCloudPB tablet_schema(schema);
    if (!document_put(txn, schema_key, std::move(tablet_schema))) {
        code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
        msg = fmt::format("failed to serialize versioned tablet schema, key={}", hex(schema_key));
    }
}

bool parse_schema_value(const ValueBuf& buf, doris::TabletSchemaCloudPB* schema) {
    // TODO(plat1ko): Apply decompression based on value version
    return buf.to_pb(schema);
}
/**
 * Processes dictionary items, mapping them to a dictionary key and adding the key to rowset meta.
 * If it's a new item, generates a new key and increments the item ID. This function is also responsible
 * for removing dynamic parts from the original RowsetMeta's TabletSchema to ensure the stability of
 * FDB schema key-value pairs.
 * 
 * @param dict The schema cloud dictionary reference, used for storing and managing schema dictionary data.
 * @param item_dict A mapping from item unique identifiers to their protobuf representations, used to find
 *                  and process specific item data.
 * @param result Pointer to the collection of result items. Stores filtered or transformed items. Can be nullptr
 *               if collecting results is not required.
 * @param items The collection of items to be processed. These items are filtered and potentially added to the dictionary.
 * @param filter A function to determine which items should be processed. If it returns true, the item is processed.
 * @param add_dict_key_fn A function to handle the logic when a new item is added to the dictionary, such as updating metadata.
 */
template <typename ItemPB>
void process_dictionary(SchemaCloudDictionary& dict,
                        const google::protobuf::Map<int32_t, ItemPB>& item_dict,
                        google::protobuf::RepeatedPtrField<ItemPB>* result,
                        const google::protobuf::RepeatedPtrField<ItemPB>& items,
                        const std::function<bool(const ItemPB&)>& filter,
                        const std::function<void(int32_t)>& add_dict_key_fn) {
    if (items.empty()) {
        return;
    }
    // Use deterministic method to do serialization since structure like
    // `google::protobuf::Map`'s serialization is unstable
    auto serialize_fn = [](const ItemPB& item) -> std::string {
        std::string output;
        google::protobuf::io::StringOutputStream string_output_stream(&output);
        google::protobuf::io::CodedOutputStream output_stream(&string_output_stream);
        output_stream.SetSerializationDeterministic(true);
        item.SerializeToCodedStream(&output_stream);
        return output;
    };

    google::protobuf::RepeatedPtrField<ItemPB> none_ext_items;
    std::unordered_map<std::string, int> reversed_dict;
    for (const auto& [key, val] : item_dict) {
        reversed_dict[serialize_fn(val)] = key;
    }

    for (const auto& item : items) {
        if (filter(item)) {
            // Filter none extended items, mainly extended columns and extended indexes
            *none_ext_items.Add() = item;
            continue;
        }
        const std::string serialized_key = serialize_fn(item);
        auto it = reversed_dict.find(serialized_key);
        if (it != reversed_dict.end()) {
            // Add existed dict key to related dict
            add_dict_key_fn(it->second);
        } else {
            // Add new dictionary key-value pair and update current_xxx_dict_id.
            int64_t current_dict_id = 0;
            if constexpr (std::is_same_v<ItemPB, ColumnPB>) {
                current_dict_id = dict.current_column_dict_id() + 1;
                dict.set_current_column_dict_id(current_dict_id);
                dict.mutable_column_dict()->emplace(current_dict_id, item);
            }
            if constexpr (std::is_same_v<ItemPB, doris::TabletIndexPB>) {
                current_dict_id = dict.current_index_dict_id() + 1;
                dict.set_current_index_dict_id(current_dict_id);
                dict.mutable_index_dict()->emplace(current_dict_id, item);
            }
            add_dict_key_fn(current_dict_id);
            reversed_dict[serialized_key] = current_dict_id;
            // LOG(INFO) << "Add dict key = " << current_dict_id << " dict value = " << item.ShortDebugString();
        }
    }
    // clear extended items to prevent writing them to fdb
    if (result != nullptr) {
        result->Swap(&none_ext_items);
    }
}

// **Notice**: Do not remove this code. We need this interface until all of the BE has been upgraded to 4.0.x
// Writes schema dictionary metadata to RowsetMetaCloudPB.
// Schema was extended in BE side, we need to reset schema to original frontend schema and store
// such restored schema in fdb. And also add extra dict key info to RowsetMetaCloudPB.
void write_schema_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                       Transaction* txn, RowsetMetaCloudPB* rowset_meta) {
    // if schema_dict_key_list is not empty, then the schema already replaced in BE side, and no need to update dict
    if (rowset_meta->has_schema_dict_key_list()) {
        return;
    }
    std::stringstream ss;
    // wrtie dict to rowset meta and update dict
    SchemaCloudDictionary dict;
    std::string dict_key = meta_schema_pb_dictionary_key({instance_id, rowset_meta->index_id()});
    ValueBuf dict_val;
    auto err = cloud::blob_get(txn, dict_key, &dict_val);
    LOG(INFO) << "Retrieved column pb dictionary, index_id=" << rowset_meta->index_id()
              << " key=" << hex(dict_key) << " error=" << err;
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND && err != TxnErrorCode::TXN_OK) {
        // Handle retrieval error.
        ss << "Failed to retrieve column pb dictionary, instance_id=" << instance_id
           << " table_id=" << rowset_meta->index_id() << " key=" << hex(dict_key)
           << " error=" << err;
        msg = ss.str();
        code = cast_as<ErrCategory::READ>(err);
        return;
    }
    if (err == TxnErrorCode::TXN_OK && !dict_val.to_pb(&dict)) {
        // Handle parse error.
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = fmt::format("Malformed tablet dictionary value, key={}", hex(dict_key));
        return;
    }

    // collect sparse columns and clear in parent column
    google::protobuf::RepeatedPtrField<ColumnPB> sparse_columns;
    for (auto& column_pb : *rowset_meta->mutable_tablet_schema()->mutable_column()) {
        if (column_pb.type() == VARIANT_TYPE_NAME && !column_pb.sparse_columns().empty()) {
            // set parent_id for restore info
            for (auto& sparse_col : *column_pb.mutable_sparse_columns()) {
                sparse_col.set_parent_unique_id(column_pb.unique_id());
            }
            sparse_columns.Add(column_pb.sparse_columns().begin(),
                               column_pb.sparse_columns().end());
        }
        // clear sparse columns to prevent writing them to fdb
        column_pb.clear_sparse_columns();
    }
    auto* dict_list = rowset_meta->mutable_schema_dict_key_list();
    // handle column dict
    auto original_column_dict_id = dict.current_column_dict_id();
    auto column_filter = [&](const doris::ColumnPB& col) -> bool { return col.unique_id() >= 0; };
    auto column_dict_adder = [&](int32_t key) { dict_list->add_column_dict_key_list(key); };
    process_dictionary<doris::ColumnPB>(
            dict, dict.column_dict(), rowset_meta->mutable_tablet_schema()->mutable_column(),
            rowset_meta->tablet_schema().column(), column_filter, column_dict_adder);

    // handle sparse column dict
    auto sparse_column_dict_adder = [&](int32_t key) {
        dict_list->add_sparse_column_dict_key_list(key);
    };
    // not filter any
    auto sparse_column_filter = [&](const doris::ColumnPB& col) -> bool { return false; };
    process_dictionary<doris::ColumnPB>(dict, dict.column_dict(), nullptr, sparse_columns,
                                        sparse_column_filter, sparse_column_dict_adder);

    // handle index info dict
    auto original_index_dict_id = dict.current_index_dict_id();
    auto index_filter = [&](const doris::TabletIndexPB& index_pb) -> bool {
        return index_pb.index_suffix_name().empty();
    };
    auto index_dict_adder = [&](int32_t key) { dict_list->add_index_info_dict_key_list(key); };
    process_dictionary<doris::TabletIndexPB>(
            dict, dict.index_dict(), rowset_meta->mutable_tablet_schema()->mutable_index(),
            rowset_meta->tablet_schema().index(), index_filter, index_dict_adder);

    // Write back modified dictionaries.
    if (original_index_dict_id != dict.current_index_dict_id() ||
        original_column_dict_id != dict.current_column_dict_id()) {
        // If dictionary was modified, serialize and save it.
        std::string dict_val;
        if (!dict.SerializeToString(&dict_val)) {
            // Handle serialization error.
            code = MetaServiceCode::PROTOBUF_SERIALIZE_ERR;
            ss << "Failed to serialize dictionary for saving, txn_id=" << rowset_meta->txn_id();
            msg = ss.str();
            return;
        }
        // Limit the size of dict value
        if (dict_val.size() > config::schema_dict_kv_size_limit) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "Failed to write dictionary for saving, txn_id=" << rowset_meta->txn_id()
               << ", reached the limited size threshold of SchemaDictKeyList "
               << config::schema_dict_kv_size_limit;
            msg = ss.str();
            return;
        }
        // Limit the count of dict keys
        if (dict.column_dict_size() > config::schema_dict_key_count_limit) {
            code = MetaServiceCode::INVALID_ARGUMENT;
            ss << "Reached max column size limit " << config::schema_dict_key_count_limit
               << ", txn_id=" << rowset_meta->txn_id();
            msg = ss.str();
            return;
        }
        // splitting large values (>90*1000) into multiple KVs
        cloud::blob_put(txn, dict_key, dict_val, 0);
        LOG(INFO) << "Dictionary saved, key=" << hex(dict_key)
                  << " txn_id=" << rowset_meta->txn_id() << " Dict size=" << dict.column_dict_size()
                  << ", index_id=" << rowset_meta->index_id()
                  << ", Current column ID=" << dict.current_column_dict_id()
                  << ", Current index ID=" << dict.current_index_dict_id()
                  << ", Dict bytes=" << dict_val.size();
    }
}

void read_schema_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                      int64_t index_id, Transaction* txn,
                      google::protobuf::RepeatedPtrField<doris::RowsetMetaCloudPB>* rsp_metas,
                      SchemaCloudDictionary* rsp_dict, GetRowsetRequest::SchemaOp schema_op) {
    std::stringstream ss;
    // read dict if any rowset has dict key list
    SchemaCloudDictionary dict;
    std::string column_dict_key = meta_schema_pb_dictionary_key({instance_id, index_id});
    ValueBuf dict_val;
    auto err = cloud::blob_get(txn, column_dict_key, &dict_val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        code = cast_as<ErrCategory::READ>(err);
        ss << "internal error, failed to get dict, err=" << err;
        msg = ss.str();
        return;
    }
    if (err == TxnErrorCode::TXN_OK && !dict_val.to_pb(&dict)) [[unlikely]] {
        code = MetaServiceCode::PROTOBUF_PARSE_ERR;
        msg = "failed to parse SchemaCloudDictionary";
        return;
    }
    LOG(INFO) << "Get schema_dict, column size=" << dict.column_dict_size()
              << ", index size=" << dict.index_dict_size();

    // Return dict, let backend to fill schema with dict info
    if (schema_op == GetRowsetRequest::RETURN_DICT && rsp_dict != nullptr) {
        rsp_dict->Swap(&dict);
        return;
    }

    auto fill_schema_with_dict = [&](RowsetMetaCloudPB* out) {
        std::unordered_map<int32_t, ColumnPB*> unique_id_map;
        //init map
        for (ColumnPB& column : *out->mutable_tablet_schema()->mutable_column()) {
            unique_id_map[column.unique_id()] = &column;
        }
        // column info
        for (size_t i = 0; i < out->schema_dict_key_list().column_dict_key_list_size(); ++i) {
            int dict_key = out->schema_dict_key_list().column_dict_key_list(i);
            const ColumnPB& dict_val = dict.column_dict().at(dict_key);
            ColumnPB& to_add = *out->mutable_tablet_schema()->add_column();
            to_add = dict_val;
            VLOG_DEBUG << "fill dict column " << dict_val.ShortDebugString();
        }

        // index info
        for (size_t i = 0; i < out->schema_dict_key_list().index_info_dict_key_list_size(); ++i) {
            int dict_key = out->schema_dict_key_list().index_info_dict_key_list(i);
            const doris::TabletIndexPB& dict_val = dict.index_dict().at(dict_key);
            doris::TabletIndexPB& to_add = *out->mutable_tablet_schema()->add_index();
            to_add = dict_val;
            VLOG_DEBUG << "fill dict index " << dict_val.ShortDebugString();
        }

        // sparse column info
        for (size_t i = 0; i < out->schema_dict_key_list().sparse_column_dict_key_list_size();
             ++i) {
            int dict_key = out->schema_dict_key_list().sparse_column_dict_key_list(i);
            const ColumnPB& dict_val = dict.column_dict().at(dict_key);
            *unique_id_map.at(dict_val.parent_unique_id())->add_sparse_columns() = dict_val;
            VLOG_DEBUG << "fill dict sparse column" << dict_val.ShortDebugString();
        }
    };

    // fill rowsets's schema with dict info
    for (auto& rowset_meta : *rsp_metas) {
        if (rowset_meta.has_schema_dict_key_list()) {
            fill_schema_with_dict(&rowset_meta);
        }
    }
}

} // namespace doris::cloud
