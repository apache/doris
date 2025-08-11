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

#include "cloud/schema_cloud_dictionary_cache.h"

#include <fmt/core.h>
#include <gen_cpp/olap_file.pb.h>
#include <vec/common/schema_util.h>

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "gen_cpp/cloud.pb.h" // For GetSchemaDictResponse
#include "runtime/exec_env.h"

namespace doris {

bvar::Adder<int64_t> g_schema_dict_cache_count("schema_dict_cache_count");
bvar::Adder<int64_t> g_replace_dict_keys_to_schema_hit_cache(
        "schema_dict_cache_replace_dict_keys_to_schema_hit_count");
bvar::Adder<int64_t> g_replace_schema_to_dict_keys_hit_cache(
        "schema_dict_cache_replace_schema_to_dict_keys_hit_count");
bvar::Adder<int64_t> g_schema_dict_cache_miss_count("schema_dict_cache_miss_count");
bvar::Adder<int64_t> g_schema_dict_refresh_count("schema_dict_refresh_count");

void SchemaCloudDictionaryCache::_insert(int64_t index_id, const SchemaCloudDictionarySPtr& dict) {
    auto* value = new CacheValue;
    value->dict = dict;
    auto* lru_handle =
            LRUCachePolicy::insert(fmt::format("{}", index_id), value, 1, 0, CachePriority::NORMAL);
    g_schema_dict_cache_count << 1;
    _cache->release(lru_handle);
}

SchemaCloudDictionarySPtr SchemaCloudDictionaryCache::_lookup(int64_t index_id) {
    Cache::Handle* handle = LRUCachePolicy::lookup(fmt::format("{}", index_id));
    if (!handle) {
        return nullptr;
    }
    auto* cache_val = static_cast<CacheValue*>(_cache->value(handle));
    SchemaCloudDictionarySPtr dict = cache_val ? cache_val->dict : nullptr;
    _cache->release(handle); // release handle but dict's shared_ptr still alive
    return dict;
}

Status check_path_amibigus(const SchemaCloudDictionary& schema, RowsetMetaCloudPB* rowset_meta) {
    // if enable_variant_flatten_nested is false, then we don't need to check path amibigus
    if (!rowset_meta->tablet_schema().enable_variant_flatten_nested()) {
        return Status::OK();
    }
    // try to get all the paths in the rowset meta
    vectorized::PathsInData all_paths;
    for (const auto& column : rowset_meta->tablet_schema().column()) {
        vectorized::PathInData path_in_data;
        path_in_data.from_protobuf(column.column_path_info());
        all_paths.push_back(path_in_data);
    }
    // try to get all the paths in the schema dict
    for (const auto& [_, column] : schema.column_dict()) {
        vectorized::PathInData path_in_data;
        path_in_data.from_protobuf(column.column_path_info());
        all_paths.push_back(path_in_data);
    }
    RETURN_IF_ERROR(vectorized::schema_util::check_variant_has_no_ambiguous_paths(all_paths));
    return Status::OK();
}
/**
 * Processes dictionary entries by matching items from the given item map.
 * It maps items to their dictionary keys, then adds these keys to the rowset metadata.
 * If an item is missing in the dictionary, the dictionary key list in rowset meta is cleared
 * and the function returns a NotFound status.
 *
 * @tparam ItemPB The protobuf message type for dictionary items (e.g., ColumnPB or TabletIndexPB).
 * @param dict The SchemaCloudDictionary that holds the dictionary entries.
 * @param item_dict A mapping from unique identifiers to the dictionary items.
 * @param result Pointer to a repeated field where filtered (non-extended) items are stored. May be null.
 * @param items The repeated field of items in the original rowset meta.
 * @param filter A predicate that returns true if an item should be treated as an extended item and skipped.
 * @param add_dict_key_fn A function to be called for each valid item that adds its key to the rowset meta.
 * @param rowset_meta Pointer to the rowset metadata; it is cleared if any item is not found.
 *
 * @return Status::OK if all items are processed successfully; otherwise, a NotFound status.
 */
template <typename ItemPB>
Status process_dictionary(SchemaCloudDictionary& dict,
                          const google::protobuf::Map<int32_t, ItemPB>& item_dict,
                          google::protobuf::RepeatedPtrField<ItemPB>* result,
                          const google::protobuf::RepeatedPtrField<ItemPB>& items,
                          const std::function<bool(const ItemPB&)>& filter,
                          const std::function<void(int32_t)>& add_dict_key_fn,
                          RowsetMetaCloudPB* rowset_meta) {
    if (items.empty()) {
        return Status::OK();
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

    google::protobuf::RepeatedPtrField<ItemPB> none_extracted_items;
    std::unordered_map<std::string, int> reversed_dict;
    for (const auto& [key, val] : item_dict) {
        reversed_dict[serialize_fn(val)] = key;
    }

    for (const auto& item : items) {
        if (filter(item)) {
            // Filter none extended items, mainly extended columns and extended indexes
            *none_extracted_items.Add() = item;
            continue;
        }
        const std::string serialized_key = serialize_fn(item);
        auto it = reversed_dict.find(serialized_key);
        if (it == reversed_dict.end()) {
            // If any required item is missing in the dictionary, clear the dict key list and return NotFound.
            // ATTN: need to clear dict key list let MS to add key list
            rowset_meta->clear_schema_dict_key_list();
            g_schema_dict_cache_miss_count << 1;
            return Status::NotFound<false>("Not found entry in dict");
        }
        // Add existed dict key to related dict
        add_dict_key_fn(it->second);
    }
    // clear extended items to prevent writing them to fdb
    if (result != nullptr) {
        result->Swap(&none_extracted_items);
    }
    return Status::OK();
}

Status SchemaCloudDictionaryCache::replace_schema_to_dict_keys(int64_t index_id,
                                                               RowsetMetaCloudPB* rowset_meta) {
    if (!rowset_meta->has_variant_type_in_schema()) {
        return Status::OK();
    }
    // first attempt to get dict from cache
    auto dict = _lookup(index_id);
    if (!dict) {
        // if not found the dict in cache, then refresh the dict from remote meta service
        RETURN_IF_ERROR(refresh_dict(index_id, &dict));
    }
    // here we should have the dict
    DCHECK(dict);
    RETURN_IF_ERROR(check_path_amibigus(*dict, rowset_meta));
    auto* dict_list = rowset_meta->mutable_schema_dict_key_list();
    // Process column dictionary: add keys for non-extended columns.
    auto column_filter = [&](const doris::ColumnPB& col) -> bool { return col.unique_id() >= 0; };
    auto column_dict_adder = [&](int32_t key) { dict_list->add_column_dict_key_list(key); };
    RETURN_IF_ERROR(process_dictionary<ColumnPB>(
            *dict, dict->column_dict(), rowset_meta->mutable_tablet_schema()->mutable_column(),
            rowset_meta->tablet_schema().column(), column_filter, column_dict_adder, rowset_meta));

    // Process index dictionary: add keys for indexes with an empty index_suffix_name.
    auto index_filter = [&](const doris::TabletIndexPB& index_pb) -> bool {
        return index_pb.index_suffix_name().empty();
    };
    auto index_dict_adder = [&](int32_t key) { dict_list->add_index_info_dict_key_list(key); };
    RETURN_IF_ERROR(process_dictionary<doris::TabletIndexPB>(
            *dict, dict->index_dict(), rowset_meta->mutable_tablet_schema()->mutable_index(),
            rowset_meta->tablet_schema().index(), index_filter, index_dict_adder, rowset_meta));
    g_replace_schema_to_dict_keys_hit_cache << 1;
    return Status::OK();
}

Status SchemaCloudDictionaryCache::_try_fill_schema(
        const std::shared_ptr<SchemaCloudDictionary>& dict, const SchemaDictKeyList& dict_keys,
        TabletSchemaCloudPB* schema) {
    // Process column dictionary keys
    for (int key : dict_keys.column_dict_key_list()) {
        auto it = dict->column_dict().find(key);
        if (it == dict->column_dict().end()) {
            return Status::NotFound<false>("Column dict key {} not found", key);
        }
        *schema->add_column() = it->second;
    }
    // Process index dictionary keys
    for (int key : dict_keys.index_info_dict_key_list()) {
        auto it = dict->index_dict().find(key);
        if (it == dict->index_dict().end()) {
            return Status::NotFound<false>("Index dict key {} not found", key);
        }
        *schema->add_index() = it->second;
    }
    return Status::OK();
}

Status SchemaCloudDictionaryCache::refresh_dict(int64_t index_id,
                                                SchemaCloudDictionarySPtr* new_dict) {
    // First attempt: use the current cached dictionary.
    auto refresh_dict = std::make_shared<SchemaCloudDictionary>();
    RETURN_IF_ERROR(static_cast<const CloudStorageEngine&>(ExecEnv::GetInstance()->storage_engine())
                            .meta_mgr()
                            .get_schema_dict(index_id, &refresh_dict));
    _insert(index_id, refresh_dict);
    if (new_dict != nullptr) {
        *new_dict = refresh_dict;
    }
    LOG(INFO) << "refresh dict for index_id=" << index_id;
    g_schema_dict_refresh_count << 1;
    return Status::OK();
}

Status SchemaCloudDictionaryCache::replace_dict_keys_to_schema(int64_t index_id,
                                                               RowsetMetaCloudPB* out) {
    // First attempt: use the current cached dictionary
    SchemaCloudDictionarySPtr dict = _lookup(index_id);
    Status st =
            dict ? _try_fill_schema(dict, out->schema_dict_key_list(), out->mutable_tablet_schema())
                 : Status::NotFound<false>("Schema dict not found in cache");

    // If filling fails (possibly due to outdated dictionary data), refresh the dictionary
    if (!st.ok()) {
        g_schema_dict_cache_miss_count << 1;
        RETURN_IF_ERROR(refresh_dict(index_id, &dict));
        if (!dict) {
            return Status::NotFound<false>("Schema dict not found after refresh, index_id={}",
                                           index_id);
        }
        // Retry filling the schema with the refreshed dictionary
        st = _try_fill_schema(dict, out->schema_dict_key_list(), out->mutable_tablet_schema());
    }
    g_replace_dict_keys_to_schema_hit_cache << 1;
    return st;
}

} // namespace doris
