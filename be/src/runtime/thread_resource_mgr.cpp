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

#include "runtime/thread_resource_mgr.h"

#include <boost/algorithm/string.hpp>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "util/cpu_info.h"

namespace doris {

ThreadResourceMgr::ThreadResourceMgr(int threads_quota) {
    DCHECK_GE(threads_quota, 0);

    if (threads_quota == 0) {
        _system_threads_quota = CpuInfo::num_cores() * config::num_threads_per_core;
    } else {
        _system_threads_quota = threads_quota;
    }

    _per_pool_quota = 0;
}

ThreadResourceMgr::ThreadResourceMgr() {
    _system_threads_quota = CpuInfo::num_cores() * config::num_threads_per_core;
    _per_pool_quota = 0;
}

ThreadResourceMgr::~ThreadResourceMgr() {
    for (auto pool : _free_pool_objs) {
        delete pool;
    }
    for (auto pool : _pools) {
        delete pool;
    }
}

ThreadResourceMgr::ResourcePool::ResourcePool(ThreadResourceMgr* parent) : _parent(parent) {}

void ThreadResourceMgr::ResourcePool::reset() {
    _num_threads = 0;
    _num_reserved_optional_threads = 0;
    _thread_available_fn = NULL;
    _max_quota = INT_MAX;
}

void ThreadResourceMgr::ResourcePool::reserve_optional_tokens(int num) {
    DCHECK_GE(num, 0);
    _num_reserved_optional_threads = num;
}

ThreadResourceMgr::ResourcePool* ThreadResourceMgr::register_pool() {
    boost::unique_lock<boost::mutex> l(_lock);
    ResourcePool* pool = NULL;

    if (_free_pool_objs.empty()) {
        pool = new ResourcePool(this);
    } else {
        pool = _free_pool_objs.front();
        _free_pool_objs.pop_front();
    }

    DCHECK(pool != NULL);
    DCHECK(_pools.find(pool) == _pools.end());
    _pools.insert(pool);
    pool->reset();

    // Added a new pool, update the quotas for each pool.
    update_pool_quotas(pool);
    return pool;
}

void ThreadResourceMgr::unregister_pool(ResourcePool* pool) {
    DCHECK(pool != NULL);
    boost::unique_lock<boost::mutex> l(_lock);
    // this may be double unregistered after pr #3326 by LaiYingChun, so check if the pool is already unregisted
    if (_pools.find(pool) != _pools.end()) {
        _pools.erase(pool);
        _free_pool_objs.push_back(pool);
        update_pool_quotas();
    }
}

void ThreadResourceMgr::ResourcePool::set_thread_available_cb(thread_available_cb fn) {
    boost::unique_lock<boost::mutex> l(_lock);
    DCHECK(_thread_available_fn == NULL || fn == NULL);
    _thread_available_fn = fn;
}

void ThreadResourceMgr::update_pool_quotas(ResourcePool* new_pool) {
    if (_pools.empty()) {
        return;
    }

    _per_pool_quota = ceil(static_cast<double>(_system_threads_quota) / _pools.size());

    for (Pools::iterator it = _pools.begin(); it != _pools.end(); ++it) {
        ResourcePool* pool = *it;

        if (pool == new_pool) {
            continue;
        }

        boost::unique_lock<boost::mutex> l(pool->_lock);

        if (pool->num_available_threads() > 0 && pool->_thread_available_fn != NULL) {
            pool->_thread_available_fn(pool);
        }
    }
}

} // namespace doris
