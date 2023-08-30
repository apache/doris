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

#include "util/runtime_profile.h"

namespace doris {

static constexpr int32_t CACHE_MIN_FREE_SIZE = 67108864; // 64M

// Base of all caches. register to CacheManager when cache is constructed.
class CachePolicy {
public:
    enum class Type {
        DATA_PAGE_CACHE = 0,
        INDEXPAGE_CACHE = 1,
        PK_INDEX_PAGE_CACHE = 2,
        SCHEMA_CACHE = 3,
        SEGMENT_CACHE = 4,
        INVERTEDINDEX_SEARCHER_CACHE = 5,
        INVERTEDINDEX_QUERY_CACHE = 6,
        LOOKUP_CONNECTION_CACHE = 7
    };

    static std::string type_string(Type type) {
        switch (type) {
        case Type::DATA_PAGE_CACHE:
            return "DataPageCache";
        case Type::INDEXPAGE_CACHE:
            return "IndexPageCache";
        case Type::PK_INDEX_PAGE_CACHE:
            return "PKIndexPageCache";
        case Type::SCHEMA_CACHE:
            return "SchemaCache";
        case Type::SEGMENT_CACHE:
            return "SegmentCache";
        case Type::INVERTEDINDEX_SEARCHER_CACHE:
            return "InvertedIndexSearcherCache";
        case Type::INVERTEDINDEX_QUERY_CACHE:
            return "InvertedIndexQueryCache";
        case Type::LOOKUP_CONNECTION_CACHE:
            return "LookupConnectionCache";
        default:
            LOG(FATAL) << "not match type of cache policy :" << static_cast<int>(type);
        }
        __builtin_unreachable();
    }

    CachePolicy(Type type, uint32_t stale_sweep_time_s);
    virtual ~CachePolicy();

    virtual void prune_stale() = 0;
    virtual void prune_all(bool clear) = 0;

    Type type() { return _type; }
    RuntimeProfile* profile() { return _profile.get(); }

protected:
    void init_profile() {
        _profile =
                std::make_unique<RuntimeProfile>(fmt::format("Cache type={}", type_string(_type)));
        _prune_stale_number_counter = ADD_COUNTER(_profile, "PruneStaleNumber", TUnit::UNIT);
        _prune_all_number_counter = ADD_COUNTER(_profile, "PruneAllNumber", TUnit::UNIT);
        _freed_memory_counter = ADD_COUNTER(_profile, "FreedMemory", TUnit::BYTES);
        _freed_entrys_counter = ADD_COUNTER(_profile, "FreedEntrys", TUnit::UNIT);
        _cost_timer = ADD_TIMER(_profile, "CostTime");
    }

    Type _type;
    std::list<CachePolicy*>::iterator _it;

    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _prune_stale_number_counter = nullptr;
    RuntimeProfile::Counter* _prune_all_number_counter = nullptr;
    // Reset before each gc
    RuntimeProfile::Counter* _freed_memory_counter = nullptr;
    RuntimeProfile::Counter* _freed_entrys_counter = nullptr;
    RuntimeProfile::Counter* _cost_timer = nullptr;

    uint32_t _stale_sweep_time_s;
};

} // namespace doris
