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

#include <ctime>
#include <memory>
#include <mutex>

#include "gutil/ref_counted.h"
#include "runtime/routine_load/data_consumer.h"
#include "util/countdown_latch.h"
#include "util/lru_cache.hpp"
#include "util/thread.h"

namespace doris {

class DataConsumer;
class DataConsumerGroup;
class Status;

// DataConsumerPool saves all available data consumer
// to be reused
class DataConsumerPool {
public:
    DataConsumerPool(int64_t max_pool_size)
            : _max_pool_size(max_pool_size), _stop_background_threads_latch(1) {}

    ~DataConsumerPool() {
        _stop_background_threads_latch.count_down();
        if (_clean_idle_consumer_thread) {
            _clean_idle_consumer_thread->join();
        }
    }

    // get a already initialized consumer from cache,
    // if not found in cache, create a new one.
    Status get_consumer(StreamLoadContext* ctx, std::shared_ptr<DataConsumer>* ret);

    // get several consumers and put them into group
    Status get_consumer_grp(StreamLoadContext* ctx, std::shared_ptr<DataConsumerGroup>* ret);

    // return the consumer to the pool
    void return_consumer(std::shared_ptr<DataConsumer> consumer);
    // return the consumers in consumer group to the pool
    void return_consumers(DataConsumerGroup* grp);

    Status start_bg_worker();

private:
    void _clean_idle_consumer_bg();

private:
    std::mutex _lock;
    std::list<std::shared_ptr<DataConsumer>> _pool;
    int64_t _max_pool_size;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _clean_idle_consumer_thread;
};

} // end namespace doris
