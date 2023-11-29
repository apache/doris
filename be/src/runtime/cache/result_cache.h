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

#include <cstdio>
#include <shared_mutex>
#include <unordered_map>

#include "gutil/integral_types.h"
#include "runtime/cache/result_node.h"
#include "util/uid_util.h"

namespace doris {
class PCacheResponse;
class PClearCacheRequest;
class PFetchCacheRequest;
class PFetchCacheResult;
class PUpdateCacheRequest;

typedef std::unordered_map<UniqueId, ResultNode*> ResultNodeMap;

// a doubly linked list class, point to result node
class ResultNodeList {
public:
    ResultNodeList() : _head(nullptr), _tail(nullptr), _node_count(0) {}
    virtual ~ResultNodeList() {}

    ResultNode* new_node(const UniqueId& sql_key) { return new ResultNode(sql_key); }

    void delete_node(ResultNode** node);

    ResultNode* pop();
    void move_tail(ResultNode* node);
    //Just remove node from link, do not delete node
    void remove(ResultNode* node);
    void push_back(ResultNode* node);
    void clear();

    ResultNode* get_head() const { return _head; }

    ResultNode* get_tail() const { return _tail; }

    size_t get_node_count() const { return _node_count; }

private:
    ResultNode* _head = nullptr;
    ResultNode* _tail = nullptr;
    size_t _node_count;
};

/**
 * Cache results of query, including the entire result set or the result set of divided partitions.
 * Two data structures, one is unordered_map and the other is a doubly linked list, corresponding to a result node.
 * If the cache is hit, the node will be moved to the end of the linked list.
 * If the cache is cleared, nodes that are expired or have not been accessed for a long time will be cleared.
 */
class ResultCache {
public:
    ResultCache(int32 max_size, int32 elasticity_size) {
        _max_size = static_cast<size_t>(max_size) * 1024 * 1024;
        _elasticity_size = static_cast<size_t>(elasticity_size) * 1024 * 1024;
        _cache_size = 0;
        _node_count = 0;
        _partition_count = 0;
    }

    virtual ~ResultCache() {}
    void update(const PUpdateCacheRequest* request, PCacheResponse* response);
    void fetch(const PFetchCacheRequest* request, PFetchCacheResult* result);
    bool contains(const UniqueId& sql_key);
    void clear(const PClearCacheRequest* request, PCacheResponse* response);

    size_t get_cache_size() { return _cache_size; }

private:
    void prune();
    void remove(ResultNode* result_node);
    void update_monitor();

    //At the same time, multithreaded reading
    //Single thread updating and cleaning(only single be, Fe is not affected)
    mutable std::shared_mutex _cache_mtx;
    ResultNodeMap _node_map;
    //List of result nodes corresponding to SqlKey,last recently used at the tail
    ResultNodeList _node_list;
    size_t _cache_size;
    size_t _max_size;
    double _elasticity_size;
    size_t _node_count;
    size_t _partition_count;

private:
    ResultCache();
    ResultCache(const ResultCache&);
    const ResultCache& operator=(const ResultCache&);
};

} // namespace doris
