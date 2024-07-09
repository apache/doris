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
#include <type_traits>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

namespace doris::cloud {
namespace config {
extern int16_t meta_schema_value_version;
}

constexpr static const char* VARIANT_TYPE_NAME = "VARIANT";

void put_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                   std::string_view schema_key, const doris::TabletSchemaCloudPB& schema) {
    TxnErrorCode err = cloud::key_exists(txn, schema_key);
    if (err == TxnErrorCode::TXN_OK) { // schema has already been saved
        TEST_SYNC_POINT_RETURN_WITH_VOID("put_schema_kv:schema_key_exists_return");
        DCHECK([&] {
            auto transform = [](std::string_view type) -> std::string_view {
                if (type == "DECIMALV2") return "DECIMAL";
                if (type == "BITMAP") return "OBJECT";
                return type;
            };
            ValueBuf buf;
            auto err = cloud::get(txn, schema_key, &buf);
            if (err != TxnErrorCode::TXN_OK) {
                LOG(WARNING) << "failed to get schema, err=" << err;
                return false;
            }
            doris::TabletSchemaCloudPB saved_schema;
            if (!buf.to_pb(&saved_schema)) {
                LOG(WARNING) << "failed to parse schema value";
                return false;
            }
            if (saved_schema.column_size() != schema.column_size()) {
                LOG(WARNING) << "saved_schema.column_size()=" << saved_schema.column_size()
                             << " schema.column_size()=" << schema.column_size();
                return false;
            }
            // Sort by column id
            std::sort(saved_schema.mutable_column()->begin(), saved_schema.mutable_column()->end(),
                      [](auto& c1, auto& c2) { return c1.unique_id() < c2.unique_id(); });
            auto& schema_ref = const_cast<doris::TabletSchemaCloudPB&>(schema);
            std::sort(schema_ref.mutable_column()->begin(), schema_ref.mutable_column()->end(),
                      [](auto& c1, auto& c2) { return c1.unique_id() < c2.unique_id(); });
            for (int i = 0; i < saved_schema.column_size(); ++i) {
                auto& saved_column = saved_schema.column(i);
                auto& column = schema.column(i);
                if (saved_column.unique_id() != column.unique_id() ||
                    transform(saved_column.type()) != transform(column.type())) {
                    LOG(WARNING) << "existed column: " << saved_column.DebugString()
                                 << "\nto save column: " << column.DebugString();
                    return false;
                }
            }
            if (saved_schema.index_size() != schema.index_size()) {
                LOG(WARNING) << "saved_schema.index_size()=" << saved_schema.index_size()
                             << " schema.index_size()=" << schema.index_size();
                return false;
            }
            // Sort by index id
            std::sort(saved_schema.mutable_index()->begin(), saved_schema.mutable_index()->end(),
                      [](auto& i1, auto& i2) { return i1.index_id() < i2.index_id(); });
            std::sort(schema_ref.mutable_index()->begin(), schema_ref.mutable_index()->end(),
                      [](auto& i1, auto& i2) { return i1.index_id() < i2.index_id(); });
            for (int i = 0; i < saved_schema.index_size(); ++i) {
                auto& saved_index = saved_schema.index(i);
                auto& index = schema.index(i);
                if (saved_index.index_id() != index.index_id() ||
                    saved_index.index_type() != index.index_type()) {
                    LOG(WARNING) << "existed index: " << saved_index.DebugString()
                                 << "\nto save index: " << index.DebugString();
                    return false;
                }
            }
            return true;
        }()) << hex(schema_key)
             << "\n to_save: " << schema.ShortDebugString();
        return;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        msg = fmt::format("failed to check that key exists, err={}", err);
        code = cast_as<ErrCategory::READ>(err);
        return;
    }
    LOG_INFO("put schema kv").tag("key", hex(schema_key));
    uint8_t ver = config::meta_schema_value_version;
    if (ver > 0) {
        cloud::put(txn, schema_key, schema, ver);
    } else {
        auto schema_value = schema.SerializeAsString();
        txn->put(schema_key, schema_value);
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

// Writes schema dictionary metadata to RowsetMetaCloudPB.
// Schema was extended in BE side, we need to reset schema to original frontend schema and store
// such restored schema in fdb. And also add extra dict key info to RowsetMetaCloudPB.
void write_schema_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                       Transaction* txn, RowsetMetaCloudPB* rowset_meta) {
    std::stringstream ss;
    // wrtie dict to rowset meta and update dict
    SchemaCloudDictionary dict;
    std::string dict_key = meta_schema_pb_dictionary_key({instance_id, rowset_meta->index_id()});
    ValueBuf dict_val;
    auto err = cloud::get(txn, dict_key, &dict_val);
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
            code = MetaServiceCode::KV_TXN_COMMIT_ERR;
            ss << "Failed to write dictionary for saving, txn_id=" << rowset_meta->txn_id()
               << ", reached the limited size threshold of SchemaDictKeyList "
               << config::schema_dict_kv_size_limit;
            msg = ss.str();
        }
        // splitting large values (>90*1000) into multiple KVs
        cloud::put(txn, dict_key, dict_val, 0);
        LOG(INFO) << "Dictionary saved, key=" << hex(dict_key)
                  << " txn_id=" << rowset_meta->txn_id() << " Dict size=" << dict.column_dict_size()
                  << ", Current column ID=" << dict.current_column_dict_id()
                  << ", Current index ID=" << dict.current_index_dict_id();
    }
}

void read_schema_from_dict(MetaServiceCode& code, std::string& msg, const std::string& instance_id,
                           int64_t index_id, Transaction* txn,
                           google::protobuf::RepeatedPtrField<RowsetMetaCloudPB>* rowset_metas) {
    std::stringstream ss;

    // read dict if any rowset has dict key list
    SchemaCloudDictionary dict;
    std::string column_dict_key = meta_schema_pb_dictionary_key({instance_id, index_id});
    ValueBuf dict_val;
    auto err = cloud::get(txn, column_dict_key, &dict_val);
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
    for (auto& rowset_meta : *rowset_metas) {
        if (rowset_meta.has_schema_dict_key_list()) {
            fill_schema_with_dict(&rowset_meta);
        }
    }
}

} // namespace doris::cloud
