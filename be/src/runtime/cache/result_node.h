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

#ifndef DORIS_BE_SRC_RUNTIME_RESULT_NODE_H
#define DORIS_BE_SRC_RUNTIME_RESULT_NODE_H

#include <sys/time.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <list>
#include <map>
#include <string>

#include "common/config.h"
#include "gen_cpp/internal_service.pb.h"
#include "olap/olap_define.h"
#include "runtime/cache/cache_utils.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/uid_util.h"

namespace doris {

class PCacheParam;
class PCacheValue;
class PCacheResponse;
class PFetchCacheRequest;
class PFetchCacheResult;
class PUpdateCacheRequest;
class PClearCacheRequest;

/**
* Cache one partition data, request param must match version and time of cache
*/
class PartitionRowBatch {
public:
    PartitionRowBatch(int64 partition_key)
            : _partition_key(partition_key), _cache_value(nullptr), _data_size(0) {}

    ~PartitionRowBatch() {}

    void set_row_batch(const PCacheValue& value);
    bool is_hit_cache(const PCacheParam& param);
    void clear();

    int64 get_partition_key() const { return _partition_key; }

    PCacheValue* get_value() { return _cache_value; }

    size_t get_data_size() { return _data_size; }

    const CacheStat* get_stat() const { return &_cache_stat; }

private:
    bool check_match(const PCacheParam& req_param) {
        if (req_param.partition_key() != _partition_key) {
            return false;
        }
        if (req_param.last_version() > _cache_value->param().last_version()) {
            return false;
        }
        if (req_param.last_version_time() > _cache_value->param().last_version_time()) {
            return false;
        }
        return true;
    }

    bool check_newer(const PCacheParam& up_param) {
        //for init data of sql cache
        if (up_param.last_version() == 0 || up_param.last_version_time() == 0) {
            return true;
        }
        if (up_param.last_version_time() > _cache_value->param().last_version_time()) {
            return true;
        }
        if (up_param.last_version() > _cache_value->param().last_version()) {
            return true;
        }
        return false;
    }

private:
    int64 _partition_key;
    PCacheValue* _cache_value;
    size_t _data_size;
    CacheStat _cache_stat;
};

typedef int64 PartitionKey;
typedef std::list<PartitionRowBatch*> PartitionRowBatchList;
typedef std::unordered_map<PartitionKey, PartitionRowBatch*> PartitionRowBatchMap;

/**
* Cache the result of one SQL, include many partition rowsets.
* Sql Cache: The partition ID comes from the partition last updated.
* Partition Cache: The partition ID comes from the partition scanned by query.
* The above two modes use the same cache structure.
*/
class ResultNode {
public:
    ResultNode() : _sql_key(0, 0), _prev(nullptr), _next(nullptr), _data_size(0) {}

    ResultNode(const UniqueId& sql_key)
            : _sql_key(sql_key), _prev(nullptr), _next(nullptr), _data_size(0) {}

    virtual ~ResultNode() {}

    PCacheStatus update_partition(const PUpdateCacheRequest* request, bool& is_update_firstkey);
    PCacheStatus fetch_partition(const PFetchCacheRequest* request,
                                 PartitionRowBatchList& rowBatchList, bool& is_hit_firstkey);
    PCacheStatus update_sql_cache(const PUpdateCacheRequest* request, bool& is_update_firstkey);
    PCacheStatus update_partition_cache(const PUpdateCacheRequest* request,
                                        bool& is_update_firstkey);

    size_t prune_first();
    void clear();

    ResultNode* get_prev() { return _prev; }

    void set_prev(ResultNode* prev) { _prev = prev; }

    ResultNode* get_next() { return _next; }

    void set_next(ResultNode* next) { _next = next; }

    void append(ResultNode* tail);

    void unlink();

    size_t get_partition_count() const { return _partition_list.size(); }

    size_t get_data_size() const { return _data_size; }

    UniqueId get_sql_key() { return _sql_key; }

    bool sql_key_null() { return _sql_key.hi == 0 && _sql_key.lo == 0; }

    void set_sql_key(const UniqueId& sql_key) { _sql_key = sql_key; }

    long first_partition_last_time() const {
        if (_partition_list.size() == 0) {
            return 0;
        }
        const PartitionRowBatch* first = *(_partition_list.begin());
        return first->get_stat()->last_read_time;
    }

    const CacheStat* get_first_stat() const {
        if (_partition_list.size() == 0) {
            return nullptr;
        }
        return (*(_partition_list.begin()))->get_stat();
    }

    const CacheStat* get_last_stat() const {
        if (_partition_list.size() == 0) {
            return nullptr;
        }
        return (*(_partition_list.end()--))->get_stat();
    }

private:
    mutable std::shared_mutex _node_mtx;
    UniqueId _sql_key;
    ResultNode* _prev;
    ResultNode* _next;
    size_t _data_size;
    PartitionRowBatchList _partition_list;
    PartitionRowBatchMap _partition_map;
};

} // namespace doris
#endif
