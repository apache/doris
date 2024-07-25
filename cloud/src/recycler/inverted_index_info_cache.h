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

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace doris {
class TabletSchemaCloudPB;
namespace cloud {
class TxnKv;

struct NoInvertedIndex {};
using InvertedIndexInfoV1 = std::vector<
        std::pair<int64_t /* inverted_index_id */, std::string /* index_path_suffix */>>;
struct InvertedIndexInfoV2 {};
struct UnknownInvertedIndex {};

using InvertedIndexInfo = std::variant<UnknownInvertedIndex, NoInvertedIndex, InvertedIndexInfoV1,
                                       InvertedIndexInfoV2>;

InvertedIndexInfo into_inverted_index_info(const doris::TabletSchemaCloudPB& schema);

class InvertedIndexInfoCache {
public:
    InvertedIndexInfoCache(std::string instance_id, std::shared_ptr<TxnKv> txn_kv);
    ~InvertedIndexInfoCache();

    // Return 0 if success, 1 if schema kv not found, negative for error
    int get(int64_t index_id, int32_t schema_version, InvertedIndexInfo& res);

private:
    void insert(int64_t index_id, int32_t schema_version, InvertedIndexInfo info);

    std::string instance_id_;
    std::shared_ptr<TxnKv> txn_kv_;

    using Key = std::pair<int64_t, int32_t>; // <index_id, schema_version>
    struct HashOfKey {
        size_t operator()(const Key& key) const {
            size_t seed = 0;
            seed = std::hash<int64_t> {}(key.first);
            seed = std::hash<int32_t> {}(key.second);
            return seed;
        }
    };

    std::mutex mtx_;
    // <index_id, schema_version> -> info
    std::unordered_map<Key, InvertedIndexInfo, HashOfKey> inverted_index_id_map_;
    // Store <index_id, schema_version> of schema which doesn't have inverted index
    std::unordered_set<Key, HashOfKey> schemas_without_inverted_index_;
    // Store ids of dropped materialized index
    std::unordered_set<int64_t> dropped_index_ids_;
};

} // namespace cloud
} // namespace doris
