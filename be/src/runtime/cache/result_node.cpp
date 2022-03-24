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
#include "runtime/cache/result_node.h"

#include "gen_cpp/internal_service.pb.h"
#include "runtime/cache/cache_utils.h"

namespace doris {

bool compare_partition(const PartitionRowBatch* left_node, const PartitionRowBatch* right_node) {
    return left_node->get_partition_key() < right_node->get_partition_key();
}

//return new batch size,only include the size of PRowBatch
void PartitionRowBatch::set_row_batch(const PCacheValue& value) {
    if (_cache_value != nullptr && !check_newer(value.param())) {
        LOG(WARNING) << "set old version data, cache ver:" << _cache_value->param().last_version()
                     << ",cache time:" << _cache_value->param().last_version_time()
                     << ", setdata ver:" << value.param().last_version()
                     << ",setdata time:" << value.param().last_version_time();
        return;
    }
    SAFE_DELETE(_cache_value);
    _cache_value = new PCacheValue(value);
    _data_size += _cache_value->data_size();
    _cache_stat.update();
    LOG(INFO) << "finish set row batch, row num:" << _cache_value->rows_size()
              << ", data size:" << _data_size;
}

bool PartitionRowBatch::is_hit_cache(const PCacheParam& param) {
    if (!check_match(param)) {
        return false;
    }
    _cache_stat.query();
    return true;
}

void PartitionRowBatch::clear() {
    LOG(INFO) << "clear partition rowbatch.";
    SAFE_DELETE(_cache_value);
    _partition_key = 0;
    _data_size = 0;
    _cache_stat.init();
}

/**
 * Update partition cache data, find RowBatch from partition map by partition key,
 * the partition rowbatch are stored in the order of partition keys
 */
PCacheStatus ResultNode::update_partition(const PUpdateCacheRequest* request,
                                          bool& is_update_firstkey) {
    is_update_firstkey = false;
    if (_sql_key != request->sql_key()) {
        LOG(INFO) << "no match sql_key " << request->sql_key().hi() << request->sql_key().lo();
        return PCacheStatus::PARAM_ERROR;
    }

    if (request->values_size() > config::query_cache_max_partition_count) {
        LOG(WARNING) << "too many partitions size:" << request->values_size();
        return PCacheStatus::PARAM_ERROR;
    }

    //Only one thread per SQL key can update the cache
    CacheWriteLock write_lock(_node_mtx);
    if (request->cache_type() == CacheType::SQL_CACHE) {
        return update_sql_cache(request, is_update_firstkey);
    } else {
        return update_partition_cache(request, is_update_firstkey);
    }
}

PCacheStatus ResultNode::update_sql_cache(const PUpdateCacheRequest* request,
                                          bool& is_update_firstkey) {
    PartitionRowBatch* partition = nullptr;
    if (request->values_size() > 1) {
        return PCacheStatus::PARAM_ERROR;
    }
    is_update_firstkey = true;
    const PCacheValue& value = request->values(0);
    PartitionKey partition_key = value.param().partition_key();
    // no cache exist, create new cache node
    if (_partition_map.size() == 0) {
        partition = new PartitionRowBatch(partition_key);
        partition->set_row_batch(value);
        _partition_map[partition_key] = partition;
        _partition_list.push_back(partition);
    } else {
        // compatible with previous version
        for (auto it = _partition_list.begin(); it != _partition_list.end(); it++) {
            _data_size -= (*it)->get_data_size();
        }
        // clear old cache, and create new cache node
        for (auto it = _partition_list.begin(); it != _partition_list.end();) {
            (*it)->clear();
            SAFE_DELETE(*it);
            it = _partition_list.erase(it);
        }
        _partition_map.clear();
        partition = new PartitionRowBatch(partition_key);
        partition->set_row_batch(value);
        _partition_map[partition_key] = partition;
        _partition_list.push_back(partition);
    }
    _data_size += partition->get_data_size();
    VLOG(1) << "finish update sql cache batches:" << _partition_list.size();
    return PCacheStatus::CACHE_OK;
}

PCacheStatus ResultNode::update_partition_cache(const PUpdateCacheRequest* request,
                                                bool& is_update_firstkey) {
    PartitionKey first_key = kint64max;
    if (_partition_list.size() == 0) {
        is_update_firstkey = true;
    } else {
        first_key = (*(_partition_list.begin()))->get_partition_key();
    }
    PartitionRowBatch* partition = nullptr;
    for (int i = 0; i < request->values_size(); i++) {
        const PCacheValue& value = request->values(i);
        PartitionKey partition_key = value.param().partition_key();
        if (!is_update_firstkey && partition_key <= first_key) {
            is_update_firstkey = true;
        }
        auto it = _partition_map.find(partition_key);
        if (it == _partition_map.end()) {
            partition = new PartitionRowBatch(partition_key);
            partition->set_row_batch(value);
            _partition_map[partition_key] = partition;
            _partition_list.push_back(partition);
#ifdef PARTITION_CACHE_DEV
            LOG(INFO) << "add index:" << i << ", pkey:" << partition->get_partition_key()
                      << ", list size:" << _partition_list.size()
                      << ", map size:" << _partition_map.size();
#endif
        } else {
            partition = it->second;
            _data_size -= partition->get_data_size();
            partition->set_row_batch(value);
#ifdef PARTITION_CACHE_DEV
            LOG(INFO) << "update index:" << i << ", pkey:" << partition->get_partition_key()
                      << ", list size:" << _partition_list.size()
                      << ", map size:" << _partition_map.size();
#endif
        }
        _data_size += partition->get_data_size();
    }
    _partition_list.sort(compare_partition);
    VLOG(1) << "finish update partition cache batches:" << _partition_list.size();
    while (config::query_cache_max_partition_count > 0 &&
           _partition_list.size() > config::query_cache_max_partition_count) {
        if (prune_first() == 0) {
            break;
        }
    }
    return PCacheStatus::CACHE_OK;
}

/**
* Only the range query of the key of the partition is supported, and the separated partition key query is not supported.
* Because a query can only be divided into two parts, part1 get data from cache, part2 fetch_data by scan node from BE.
* Partition cache : 20191211-20191215
* Hit cache parameter : [20191211 - 20191215], [20191212 - 20191214], [20191212 - 20191216],[20191210 - 20191215]
* Miss cache parameter: [20191210 - 20191216]
*/
PCacheStatus ResultNode::fetch_partition(const PFetchCacheRequest* request,
                                         PartitionRowBatchList& row_batch_list,
                                         bool& is_hit_firstkey) {
    is_hit_firstkey = false;
    if (request->params_size() == 0) {
        return PCacheStatus::PARAM_ERROR;
    }

    CacheReadLock read_lock(_node_mtx);

    if (_partition_list.size() == 0) {
        return PCacheStatus::NO_PARTITION_KEY;
    }

    if (request->params(0).partition_key() > (*_partition_list.rbegin())->get_partition_key() ||
        request->params(request->params_size() - 1).partition_key() <
                (*_partition_list.begin())->get_partition_key()) {
        return PCacheStatus::NO_PARTITION_KEY;
    }

    bool find = false;
    int begin_idx = -1, end_idx = -1, param_idx = 0;
    auto begin_it = _partition_list.end();
    auto end_it = _partition_list.end();
    auto part_it = _partition_list.begin();

    PCacheStatus status = PCacheStatus::CACHE_OK;
    while (param_idx < request->params_size() && part_it != _partition_list.end()) {
#ifdef PARTITION_CACHE_DEV
        LOG(INFO) << "Param index : " << param_idx
                  << ", param part Key : " << request->params(param_idx).partition_key()
                  << ", batch part key : " << (*part_it)->get_partition_key();
#endif
        if (!find) {
            while (part_it != _partition_list.end() &&
                   request->params(param_idx).partition_key() > (*part_it)->get_partition_key()) {
                part_it++;
            }
            if (part_it != _partition_list.end()) {
                while (param_idx < request->params_size() &&
                       request->params(param_idx).partition_key() <
                               (*part_it)->get_partition_key()) {
                    param_idx++;
                }
                if (param_idx < request->params_size()) {
                    if (request->params(param_idx).partition_key() ==
                        (*part_it)->get_partition_key()) {
                        find = true;
                    }
                }
            }
        }

        if (find) {
#ifdef PARTITION_CACHE_DEV
            LOG(INFO) << "Find! Param index : " << param_idx
                      << ", param part Key : " << request->params(param_idx).partition_key()
                      << ", batch part key : " << (*part_it)->get_partition_key()
                      << ", param part version : " << request->params(param_idx).last_version()
                      << ", batch part version : "
                      << (*part_it)->get_value()->param().last_version()
                      << ", param part version time : "
                      << request->params(param_idx).last_version_time()
                      << ", batch part version time : "
                      << (*part_it)->get_value()->param().last_version_time();
#endif
            if ((*part_it)->is_hit_cache(request->params(param_idx))) {
                if (begin_idx < 0) {
                    begin_idx = param_idx;
                    begin_it = part_it;
                }
                end_idx = param_idx;
                end_it = part_it;
                param_idx++;
                part_it++;
                find = false;
            } else {
                status = PCacheStatus::DATA_OVERDUE;
                break;
            }
        }
    }

    if (begin_it == _partition_list.end() && end_it == _partition_list.end()) {
        return status;
    }

    //[20191210 - 20191216] hit partition range [20191212-20191214],the sql will be splited to 3 part!
    if (begin_idx != 0 && end_idx != request->params_size() - 1) {
        return PCacheStatus::INVALID_KEY_RANGE;
    }
    if (begin_it == _partition_list.begin()) {
        is_hit_firstkey = true;
    }

    while (true) {
        row_batch_list.push_back(*begin_it);
        if (begin_it == end_it) {
            break;
        }
        begin_it++;
    }
    return status;
}

/*
* prune first partition result
*/
size_t ResultNode::prune_first() {
    if (_partition_list.size() == 0) {
        return 0;
    }
    PartitionRowBatch* part_node = *_partition_list.begin();
    size_t prune_size = part_node->get_data_size();
    _partition_list.erase(_partition_list.begin());
    _partition_map.erase(part_node->get_partition_key());
    part_node->clear();
    SAFE_DELETE(part_node);
    _data_size -= prune_size;
    return prune_size;
}

void ResultNode::clear() {
    CacheWriteLock write_lock(_node_mtx);
    LOG(INFO) << "clear result node:" << _sql_key;
    _sql_key.hi = 0;
    _sql_key.lo = 0;
    for (auto it = _partition_list.begin(); it != _partition_list.end();) {
        (*it)->clear();
        SAFE_DELETE(*it);
        it = _partition_list.erase(it);
    }
    _data_size = 0;
}

void ResultNode::append(ResultNode* tail) {
    _prev = tail;
    if (tail) tail->set_next(this);
}

void ResultNode::unlink() {
    if (_next) {
        _next->set_prev(_prev);
    }
    if (_prev) {
        _prev->set_next(_next);
    }
    _next = nullptr;
    _prev = nullptr;
}

} // namespace doris
