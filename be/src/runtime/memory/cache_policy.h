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

static constexpr int32_t CACHE_MIN_PRUNE_SIZE = 67108864; // 64M
static constexpr int32_t CACHE_MIN_PRUNE_NUMBER = 1024;

// Base of all caches. register to CacheManager when cache is constructed.
class CachePolicy {
public:
    enum class CacheType {
        DATA_PAGE_CACHE = 0,
        INDEXPAGE_CACHE = 1,
        PK_INDEX_PAGE_CACHE = 2,
        SCHEMA_CACHE = 3,
        SEGMENT_CACHE = 4,
        INVERTEDINDEX_SEARCHER_CACHE = 5,
        INVERTEDINDEX_QUERY_CACHE = 6,
        LOOKUP_CONNECTION_CACHE = 7,
        POINT_QUERY_ROW_CACHE = 8,
        DELETE_BITMAP_AGG_CACHE = 9,
        TABLET_VERSION_CACHE = 10,
        LAST_SUCCESS_CHANNEL_CACHE = 11,
        COMMON_OBJ_LRU_CACHE = 12,
        FOR_UT_CACHE_SIZE = 13,
        TABLET_SCHEMA_CACHE = 14,
        CREATE_TABLET_RR_IDX_CACHE = 15,
        CLOUD_TABLET_CACHE = 16,
        CLOUD_TXN_DELETE_BITMAP_CACHE = 17,
        NONE = 18, // not be used
        FOR_UT_CACHE_NUMBER = 19,
        QUERY_CACHE = 20
    };

    static std::string type_string(CacheType type) {
        switch (type) {
        case CacheType::DATA_PAGE_CACHE:
            return "DataPageCache";
        case CacheType::INDEXPAGE_CACHE:
            return "IndexPageCache";
        case CacheType::PK_INDEX_PAGE_CACHE:
            return "PKIndexPageCache";
        case CacheType::SCHEMA_CACHE:
            return "SchemaCache";
        case CacheType::SEGMENT_CACHE:
            return "SegmentCache";
        case CacheType::INVERTEDINDEX_SEARCHER_CACHE:
            return "InvertedIndexSearcherCache";
        case CacheType::INVERTEDINDEX_QUERY_CACHE:
            return "InvertedIndexQueryCache";
        case CacheType::LOOKUP_CONNECTION_CACHE:
            return "PointQueryLookupConnectionCache";
        case CacheType::POINT_QUERY_ROW_CACHE:
            return "PointQueryRowCache";
        case CacheType::DELETE_BITMAP_AGG_CACHE:
            return "MowDeleteBitmapAggCache";
        case CacheType::TABLET_VERSION_CACHE:
            return "MowTabletVersionCache";
        case CacheType::LAST_SUCCESS_CHANNEL_CACHE:
            return "LastSuccessChannelCache";
        case CacheType::COMMON_OBJ_LRU_CACHE:
            return "CommonObjLRUCache";
        case CacheType::FOR_UT_CACHE_SIZE:
            return "ForUTCacheSize";
        case CacheType::TABLET_SCHEMA_CACHE:
            return "TabletSchemaCache";
        case CacheType::CREATE_TABLET_RR_IDX_CACHE:
            return "CreateTabletRRIdxCache";
        case CacheType::CLOUD_TABLET_CACHE:
            return "CloudTabletCache";
        case CacheType::CLOUD_TXN_DELETE_BITMAP_CACHE:
            return "CloudTxnDeleteBitmapCache";
        case CacheType::FOR_UT_CACHE_NUMBER:
            return "ForUTCacheNumber";
        case CacheType::QUERY_CACHE:
            return "QUERY_CACHE";
        default:
            LOG(FATAL) << "not match type of cache policy :" << static_cast<int>(type);
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

    inline static std::unordered_map<std::string, CacheType> StringToType = {
            {"DataPageCache", CacheType::DATA_PAGE_CACHE},
            {"IndexPageCache", CacheType::INDEXPAGE_CACHE},
            {"PKIndexPageCache", CacheType::PK_INDEX_PAGE_CACHE},
            {"SchemaCache", CacheType::SCHEMA_CACHE},
            {"SegmentCache", CacheType::SEGMENT_CACHE},
            {"InvertedIndexSearcherCache", CacheType::INVERTEDINDEX_SEARCHER_CACHE},
            {"InvertedIndexQueryCache", CacheType::INVERTEDINDEX_QUERY_CACHE},
            {"PointQueryLookupConnectionCache", CacheType::LOOKUP_CONNECTION_CACHE},
            {"PointQueryRowCache", CacheType::POINT_QUERY_ROW_CACHE},
            {"MowDeleteBitmapAggCache", CacheType::DELETE_BITMAP_AGG_CACHE},
            {"MowTabletVersionCache", CacheType::TABLET_VERSION_CACHE},
            {"LastSuccessChannelCache", CacheType::LAST_SUCCESS_CHANNEL_CACHE},
            {"CommonObjLRUCache", CacheType::COMMON_OBJ_LRU_CACHE},
            {"ForUTCacheSize", CacheType::FOR_UT_CACHE_SIZE},
            {"TabletSchemaCache", CacheType::TABLET_SCHEMA_CACHE},
            {"CreateTabletRRIdxCache", CacheType::CREATE_TABLET_RR_IDX_CACHE},
            {"CloudTabletCache", CacheType::CLOUD_TABLET_CACHE},
            {"CloudTxnDeleteBitmapCache", CacheType::CLOUD_TXN_DELETE_BITMAP_CACHE},
            {"ForUTCacheNumber", CacheType::FOR_UT_CACHE_NUMBER}};

    static CacheType string_to_type(std::string type) {
        if (StringToType.contains(type)) {
            return StringToType[type];
        } else {
            return CacheType::NONE;
        }
    }

    CachePolicy(CacheType type, size_t capacity, uint32_t stale_sweep_time_s, bool enable_prune);
    virtual ~CachePolicy();

    virtual void prune_stale() = 0;
    virtual void prune_all(bool force) = 0;
    virtual int64_t adjust_capacity_weighted(double adjust_weighted) = 0;
    virtual size_t get_capacity() = 0;

    CacheType type() { return _type; }
    size_t initial_capacity() const { return _initial_capacity; }
    bool enable_prune() const { return _enable_prune; }
    RuntimeProfile* profile() { return _profile.get(); }

protected:
    void init_profile() {
        _profile =
                std::make_unique<RuntimeProfile>(fmt::format("Cache type={}", type_string(_type)));
        _prune_stale_number_counter = ADD_COUNTER(_profile, "PruneStaleNumber", TUnit::UNIT);
        _prune_all_number_counter = ADD_COUNTER(_profile, "PruneAllNumber", TUnit::UNIT);
        _adjust_capacity_weighted_number_counter =
                ADD_COUNTER(_profile, "SetCapacityNumber", TUnit::UNIT);
        _freed_memory_counter = ADD_COUNTER(_profile, "FreedMemory", TUnit::BYTES);
        _freed_entrys_counter = ADD_COUNTER(_profile, "FreedEntrys", TUnit::UNIT);
        _cost_timer = ADD_TIMER(_profile, "CostTime");
    }

    CacheType _type;
    size_t _initial_capacity {0};

    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _prune_stale_number_counter = nullptr;
    RuntimeProfile::Counter* _prune_all_number_counter = nullptr;
    RuntimeProfile::Counter* _adjust_capacity_weighted_number_counter = nullptr;
    // Reset before each gc
    RuntimeProfile::Counter* _freed_memory_counter = nullptr;
    RuntimeProfile::Counter* _freed_entrys_counter = nullptr;
    RuntimeProfile::Counter* _cost_timer = nullptr;

    uint32_t _stale_sweep_time_s;
    bool _enable_prune = true;
};

} // namespace doris
