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
#include <mutex>

#include "runtime/routine_load/data_consumer.h"
#include "util/lru_cache.hpp"

namespace doris {

class DataConsumer;
class Status;

// DataConsumerPool saves all available data consumer
// to be reused
class DataConsumerPool {
public:
    DataConsumerPool(int64_t max_pool_size):
        _max_pool_size(max_pool_size) {
    }

    ~DataConsumerPool() {
    }

    // get a already initialized consumer from cache,
    // if not found in cache, create a new one.
    Status get_consumer(
        StreamLoadContext* ctx,
        std::shared_ptr<DataConsumer>* ret);

    // erase the specified cache
    void return_consumer(std::shared_ptr<DataConsumer> consumer);

protected:
    std::mutex _lock;
    std::list<std::shared_ptr<DataConsumer>> _pool;
    int64_t _max_pool_size;
};

} // end namespace doris
