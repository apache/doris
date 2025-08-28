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

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <string>
#include <utility>

#include "runtime/memory/lru_cache_policy.h"

namespace doris {

class SchemaCloudDictionary;
class RowsetMetaCloudPB;

using SchemaCloudDictionarySPtr = std::shared_ptr<SchemaCloudDictionary>;

/*
 * SchemaCloudDictionaryCache provides a local cache for SchemaCloudDictionary.
 *
 * Caching logic:
 *  - If the dictionary associated with a given key has not had any new columns added 
 *    (determined by comparing the serialized data for consistency),
 *    the cached dictionary is directly used to update the dictionary list in the rowset meta
 *    (similar to the process_dictionary logic in write_schema_dict).
 *  - If new columns have been detected, the local cache is disregarded, and the updated 
 *    dictionary should be fetched via the meta service.
 */
class SchemaCloudDictionaryCache : public LRUCachePolicy {
public:
    SchemaCloudDictionaryCache(size_t capacity)
            : LRUCachePolicy(CachePolicy::CacheType::SCHEMA_CLOUD_DICTIONARY_CACHE, capacity,
                             LRUCacheType::NUMBER, 512) {}
    /**
    * Refreshes the dictionary for the given index_id by calling an RPC via the meta manager.
    * The refreshed dictionary is then inserted into the cache.
    *
    * @param index_id The identifier for the index.
    * @param new_dict Optional output parameter; if provided, it will be set to point to the refreshed dictionary.
    *
    * @return Status::OK if the dictionary is successfully refreshed; otherwise, an error status.
    */
    virtual Status refresh_dict(int64_t index_id, SchemaCloudDictionarySPtr* new_dict = nullptr);

    /**
    * Refreshes the dictionary for the given index_id by calling an RPC via the meta manager.
    * The refreshed dictionary is then inserted into the cache.
    *
    * @param index_id The identifier for the index.
    * @param new_dict Optional output parameter; if provided, it will be set to point to the refreshed dictionary.
    *
    * @return Status::OK if the dictionary is successfully refreshed; otherwise, an error status.
    */
    Status replace_schema_to_dict_keys(int64_t index_id, RowsetMetaCloudPB* out);

    /**
     * Replaces dictionary keys in the given RowsetMetaCloudPB by using the cached dictionary.
     * If the cached dictionary is missing or its data is outdated (i.e. missing required keys),
     * an RPC call is triggered to refresh the dictionary, which is then used to fill the tablet schema.
     *
     * @param index_id The identifier for the index.
     * @param out Pointer to the RowsetMetaCloudPB whose tablet schema will be updated.
     *
     * @return Status::OK if the tablet schema is successfully updated; otherwise, an error status.
     */
    Status replace_dict_keys_to_schema(int64_t index_id, RowsetMetaCloudPB* out);

private:
    // ut
    friend class FakeSchemaCloudDictionaryCache;
    // insert dict
    void _insert(int64_t index_id, const SchemaCloudDictionarySPtr& dict);
    // lookup dict
    SchemaCloudDictionarySPtr _lookup(int64_t index_id);
    // Attempts to fill the tablet schema information in a SchemaCloudDictionary into a TabletSchemaCloudPB
    // based on a given set of dictionary keys. If any required key is missing in the dictionary, a NotFound status is returned.
    Status _try_fill_schema(const SchemaCloudDictionarySPtr& dict,
                            const SchemaDictKeyList& dict_keys, TabletSchemaCloudPB* schema);
    struct CacheValue : public LRUCacheValueBase {
        SchemaCloudDictionarySPtr dict;
    };
};

} // namespace doris