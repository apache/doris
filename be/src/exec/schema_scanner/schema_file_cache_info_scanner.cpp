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

#include "exec/schema_scanner/schema_file_cache_info_scanner.h"

#include "io/cache/file_cache_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaFileCacheInfoScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"HASH", TYPE_STRING, sizeof(StringRef), true},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"TYPE", TYPE_STRING, sizeof(StringRef), true},
        {"REMOTE_PATH", TYPE_STRING, sizeof(StringRef), true},
        {"CACHE_PATH", TYPE_STRING, sizeof(StringRef), true},
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), true}};

SchemaFileCacheInfoScanner::SchemaFileCacheInfoScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_FILE_CACHE_INFO) {}

SchemaFileCacheInfoScanner::~SchemaFileCacheInfoScanner() {}

Status SchemaFileCacheInfoScanner::start(RuntimeState* state) {
    return Status::OK();
}

Status SchemaFileCacheInfoScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    return _fill_block_impl(block);
}

Status SchemaFileCacheInfoScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    auto* file_cache_factory = ExecEnv::GetInstance()->file_cache_factory();
    if (!file_cache_factory) {
        return Status::OK();
    }

    // Collect all cache entries from all file cache instances
    std::vector<std::tuple<std::string, int64_t, int64_t, int, std::string>> cache_entries;

    // Get all cache instances using the public getter
    const auto& caches = file_cache_factory->get_caches();
    for (const auto& cache : caches) {
        const std::string& cache_path = cache->get_base_path();

        // Get the storage from cache using the public getter
        auto* storage = cache->get_storage();
        if (!storage) {
            continue;
        }

        // Try to get meta_store from FSFileCacheStorage using the public getter
        auto* fs_storage = dynamic_cast<doris::io::FSFileCacheStorage*>(storage);
        if (!fs_storage) {
            continue;
        }

        auto* meta_store = fs_storage->get_meta_store();
        if (!meta_store) {
            continue;
        }

        // Get iterator for all BlockMeta records
        auto iterator = meta_store->get_all();
        if (!iterator) {
            continue;
        }

        // Iterate through all cache entries
        while (iterator->valid()) {
            const auto& key = iterator->key();
            const auto& value = iterator->value();

            // Check for deserialization errors
            if (!iterator->get_last_key_error().ok() || !iterator->get_last_value_error().ok()) {
                LOG(WARNING) << "Failed to deserialize cache block metadata: "
                             << "key_error=" << iterator->get_last_key_error().to_string()
                             << ", value_error=" << iterator->get_last_value_error().to_string();
                iterator->next();
                continue; // Skip invalid records
            }

            // Convert hash to string
            std::string hash_str = key.hash.to_string();

            // Add to cache entries
            cache_entries.emplace_back(hash_str, key.tablet_id, value.size, value.type, cache_path);

            iterator->next();
        }
    }

    const size_t row_num = cache_entries.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_tbls_columns.size(); ++col_idx) {
        const auto& col_desc = _s_tbls_columns[col_idx];

        std::vector<StringRef> str_refs(row_num);
        std::vector<int64_t> int64_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(row_num);

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& entry = cache_entries[row_idx];
            const auto& [hash, tablet_id, size, type, cache_path] = entry;

            if (col_desc.type == TYPE_STRING) {
                switch (col_idx) {
                case 0: // HASH
                    column_values[row_idx] = hash;
                    break;
                case 3: // TYPE
                    column_values[row_idx] = doris::io::cache_type_to_string(
                            static_cast<doris::io::FileCacheType>(type));
                    break;
                case 4:                          // REMOTE_PATH
                    column_values[row_idx] = ""; // TODO: Implement remote path retrieval
                    break;
                case 5: // CACHE_PATH
                    column_values[row_idx] = cache_path;
                    break;
                default:
                    column_values[row_idx] = "";
                    break;
                }
                str_refs[row_idx] =
                        StringRef(column_values[row_idx].data(), column_values[row_idx].size());
                datas[row_idx] = &str_refs[row_idx];
            } else if (col_desc.type == TYPE_BIGINT) {
                switch (col_idx) {
                case 1: // TABLET_ID
                    int64_vals[row_idx] = tablet_id;
                    break;
                case 2: // SIZE
                    int64_vals[row_idx] = size;
                    break;
                case 6: // BE_ID
                    int64_vals[row_idx] = ExecEnv::GetInstance()->cluster_info()->backend_id;
                    break;
                default:
                    int64_vals[row_idx] = 0;
                    break;
                }
                datas[row_idx] = &int64_vals[row_idx];
            }
        }

        RETURN_IF_ERROR(fill_dest_column_for_range(block, col_idx, datas));
    }

    return Status::OK();
}

} // namespace doris