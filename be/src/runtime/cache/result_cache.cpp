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
#include "runtime/cache/result_cache.h"

#include "gen_cpp/internal_service.pb.h"
#include "util/doris_metrics.h"

namespace doris {

/**
* Remove the tail node of link
*/
ResultNode* ResultNodeList::pop() {
    remove(_head);
    return _head;
}

void ResultNodeList::remove(ResultNode* node) {
    if (!node) return;
    if (node == _head) _head = node->get_next();
    if (node == _tail) _tail = node->get_prev();
    node->unlink();
    _node_count--;
}

void ResultNodeList::push_back(ResultNode* node) {
    if (!node) return;
    if (!_head) _head = node;
    node->append(_tail);
    _tail = node;
    _node_count++;
}

void ResultNodeList::move_tail(ResultNode* node) {
    if (!node || node == _tail) return;
    if (!_head)
        _head = node;
    else if (node == _head)
        _head = node->get_next();
    node->unlink();
    node->append(_tail);
    _tail = node;
}

void ResultNodeList::delete_node(ResultNode** node) {
    (*node)->clear();
    SAFE_DELETE(*node);
}

void ResultNodeList::clear() {
    LOG(INFO) << "clear result node list.";
    while (_head) {
        ResultNode* tmp_node = _head->get_next();
        _head->clear();
        SAFE_DELETE(_head);
        _head = tmp_node;
    }
    _node_count = 0;
}
/**
 * Find the node and update partition data
 * New node, the node updated in the first partition will move to the tail of the list
 */
void ResultCache::update(const PUpdateCacheRequest* request, PCacheResponse* response) {
    ResultNode* node;
    PCacheStatus status;
    bool update_first = false;
    UniqueId sql_key = request->sql_key();
    LOG(INFO) << "update cache, sql key:" << sql_key;

    CacheWriteLock write_lock(_cache_mtx);
    auto it = _node_map.find(sql_key);
    if (it != _node_map.end()) {
        node = it->second;
        _cache_size -= node->get_data_size();
        _partition_count -= node->get_partition_count();
        status = node->update_partition(request, update_first);
    } else {
        node = _node_list.new_node(sql_key);
        status = node->update_partition(request, update_first);
        _node_list.push_back(node);
        _node_map[sql_key] = node;
        _node_count += 1;
    }
    if (update_first) {
        _node_list.move_tail(node);
    }
    _cache_size += node->get_data_size();
    _partition_count += node->get_partition_count();
    response->set_status(status);

    prune();
    update_monitor();
}

/**
 * Fetch cache through sql key, partition key, version and time
 */
void ResultCache::fetch(const PFetchCacheRequest* request, PFetchCacheResult* result) {
    bool hit_first = false;
    ResultNodeMap::iterator node_it;
    const UniqueId sql_key = request->sql_key();
    LOG(INFO) << "fetch cache, sql key:" << sql_key;
    {
        CacheReadLock read_lock(_cache_mtx);
        node_it = _node_map.find(sql_key);
        if (node_it == _node_map.end()) {
            result->set_status(PCacheStatus::NO_SQL_KEY);
            LOG(INFO) << "no such sql key:" << sql_key;
            return;
        }
        ResultNode* node = node_it->second;
        PartitionRowBatchList part_rowbatch_list;
        PCacheStatus status = node->fetch_partition(request, part_rowbatch_list, hit_first);

        for (auto part_it = part_rowbatch_list.begin(); part_it != part_rowbatch_list.end();
             part_it++) {
            PCacheValue* srcValue = (*part_it)->get_value();
            if (srcValue != nullptr) {
                PCacheValue* value = result->add_values();
                value->CopyFrom(*srcValue);
                LOG(INFO) << "fetch cache partition key:" << srcValue->param().partition_key();
            } else {
                LOG(WARNING) << "prowbatch of cache is null";
                status = PCacheStatus::EMPTY_DATA;
                break;
            }
        }
        if (status == PCacheStatus::CACHE_OK && part_rowbatch_list.empty()) {
            status = PCacheStatus::EMPTY_DATA;
        }
        result->set_status(status);
    }

    if (hit_first) {
        CacheWriteLock write_lock(_cache_mtx);
        _node_list.move_tail(node_it->second);
    }
}

bool ResultCache::contains(const UniqueId& sql_key) {
    CacheReadLock read_lock(_cache_mtx);
    return _node_map.find(sql_key) != _node_map.end();
}

/**
 * enum PClearType {
 *   CLEAR_ALL = 0,
 *   PRUNE_CACHE = 1,
 *   CLEAR_BEFORE_TIME = 2,
 *   CLEAR_SQL_KEY = 3
 * };
 */
void ResultCache::clear(const PClearCacheRequest* request, PCacheResponse* response) {
    LOG(INFO) << "clear cache type" << request->clear_type()
              << ", node size:" << _node_list.get_node_count() << ", map size:" << _node_map.size();
    CacheWriteLock write_lock(_cache_mtx);
    //0 clear, 1 prune, 2 before_time,3 sql_key
    switch (request->clear_type()) {
    case PClearType::CLEAR_ALL:
        _node_list.clear();
        _node_map.clear();
        _cache_size = 0;
        _node_count = 0;
        _partition_count = 0;
        break;
    case PClearType::PRUNE_CACHE:
        prune();
        break;
    default:
        break;
    }
    update_monitor();
    response->set_status(PCacheStatus::CACHE_OK);
}

//private method
ResultNode* find_min_time_node(ResultNode* result_node) {
    if (result_node->get_prev()) {
        if (result_node->get_prev()->first_partition_last_time() <=
            result_node->first_partition_last_time()) {
            return result_node->get_prev();
        }
    }

    if (result_node->get_next()) {
        if (result_node->get_next()->first_partition_last_time() <
            result_node->first_partition_last_time()) {
            return result_node->get_next();
        }
    }
    return result_node;
}

/*
* Two-dimensional array, prune the min last_read_time PartitionRowBatch.
* The following example is the last read time array.
* 1 and 2 is the read time, nodes with pruning read time < 3
* Before:
*   1,2         //_head ResultNode*
*   1,2,3,4,5   
*   2,4,3,6,8   
*   5,7,9,11,13 //_tail ResultNode*
* After:
*   4,5         //_head
*   4,3,6,8
*   5,7,9,11,13 //_tail
*/
void ResultCache::prune() {
    if (_cache_size <= (_max_size + _elasticity_size)) {
        return;
    }
    LOG(INFO) << "begin prune cache, cache_size : " << _cache_size << ", max_size : " << _max_size
              << ", elasticity_size : " << _elasticity_size;
    ResultNode* result_node = _node_list.get_head();
    while (_cache_size > _max_size) {
        if (result_node == nullptr) {
            break;
        }
        result_node = find_min_time_node(result_node);
        _cache_size -= result_node->prune_first();
        if (result_node->get_data_size() == 0) {
            ResultNode* next_node;
            if (result_node->get_next()) {
                next_node = result_node->get_next();
            } else if (result_node->get_prev()) {
                next_node = result_node->get_prev();
            } else {
                next_node = _node_list.get_head();
            }
            remove(result_node);
            result_node = next_node;
        }
    }
    LOG(INFO) << "finish prune, cache_size : " << _cache_size;
    _node_count = _node_map.size();
    _cache_size = 0;
    _partition_count = 0;
    for (auto node_it = _node_map.begin(); node_it != _node_map.end(); node_it++) {
        _partition_count += node_it->second->get_partition_count();
        _cache_size += node_it->second->get_data_size();
    }
}

void ResultCache::remove(ResultNode* result_node) {
    auto node_it = _node_map.find(result_node->get_sql_key());
    if (node_it != _node_map.end()) {
        _node_map.erase(node_it);
        _node_list.remove(result_node);
        _node_list.delete_node(&result_node);
    }
}

void ResultCache::update_monitor() {
    DorisMetrics::instance()->query_cache_memory_total_byte->set_value(_cache_size);
    DorisMetrics::instance()->query_cache_sql_total_count->set_value(_node_count);
    DorisMetrics::instance()->query_cache_partition_total_count->set_value(_partition_count);
}

} // namespace doris
