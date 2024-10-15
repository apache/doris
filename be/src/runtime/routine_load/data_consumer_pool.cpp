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

#include "runtime/routine_load/data_consumer_pool.h"

#include <gen_cpp/Types_types.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <ctime>
#include <iterator>
#include <map>
#include <ostream>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/routine_load/data_consumer.h"
#include "runtime/routine_load/data_consumer_group.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/uid_util.h"

namespace doris {

Status DataConsumerPool::get_consumer(std::shared_ptr<StreamLoadContext> ctx,
                                      std::shared_ptr<DataConsumer>* ret) {
    std::unique_lock<std::mutex> l(_lock);

    // check if there is an available consumer.
    // if has, return it, also remove it from the pool
    auto iter = std::begin(_pool);
    while (iter != std::end(_pool)) {
        if ((*iter)->match(ctx)) {
            VLOG_NOTICE << "get an available data consumer from pool: " << (*iter)->id();
            static_cast<void>((*iter)->reset());
            *ret = *iter;
            iter = _pool.erase(iter);
            return Status::OK();
        } else {
            ++iter;
        }
    }

    // no available consumer, create a new one
    std::shared_ptr<DataConsumer> consumer;
    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA:
        consumer = std::make_shared<KafkaDataConsumer>(ctx);
        break;
    default:
        return Status::InternalError("PAUSE: unknown routine load task type: {}", ctx->load_type);
    }

    // init the consumer
    RETURN_IF_ERROR(consumer->init(ctx));

    VLOG_NOTICE << "create new data consumer: " << consumer->id();
    *ret = consumer;
    return Status::OK();
}

Status DataConsumerPool::get_consumer_grp(std::shared_ptr<StreamLoadContext> ctx,
                                          std::shared_ptr<DataConsumerGroup>* ret) {
    if (ctx->load_src_type != TLoadSourceType::KAFKA) {
        return Status::InternalError(
                "PAUSE: Currently only support consumer group for Kafka data source");
    }
    DCHECK(ctx->kafka_info);

    if (ctx->kafka_info->begin_offset.size() == 0) {
        return Status::InternalError("PAUSE: The size of begin_offset of task should not be 0.");
    }

    // one data consumer group contains at least one data consumers.
    int max_consumer_num = config::max_consumer_num_per_group;
    size_t consumer_num = std::min((size_t)max_consumer_num, ctx->kafka_info->begin_offset.size());

    std::shared_ptr<KafkaDataConsumerGroup> grp =
            std::make_shared<KafkaDataConsumerGroup>(consumer_num);

    for (int i = 0; i < consumer_num; ++i) {
        std::shared_ptr<DataConsumer> consumer;
        RETURN_IF_ERROR(get_consumer(ctx, &consumer));
        grp->add_consumer(consumer);
    }

    LOG(INFO) << "get consumer group " << grp->grp_id() << " with " << consumer_num << " consumers";
    *ret = grp;
    return Status::OK();
}

void DataConsumerPool::return_consumer(std::shared_ptr<DataConsumer> consumer) {
    std::unique_lock<std::mutex> l(_lock);

    if (_pool.size() == config::routine_load_consumer_pool_size) {
        VLOG_NOTICE << "data consumer pool is full: " << _pool.size() << "-"
                    << config::routine_load_consumer_pool_size
                    << ", discard the returned consumer: " << consumer->id();
        return;
    }

    static_cast<void>(consumer->reset());
    _pool.push_back(consumer);
    VLOG_NOTICE << "return the data consumer: " << consumer->id()
                << ", current pool size: " << _pool.size();
}

void DataConsumerPool::return_consumers(DataConsumerGroup* grp) {
    for (std::shared_ptr<DataConsumer> consumer : grp->consumers()) {
        return_consumer(consumer);
    }
}

Status DataConsumerPool::start_bg_worker() {
    RETURN_IF_ERROR(Thread::create(
            "ResultBufferMgr", "clean_idle_consumer",
            [this]() {
                do {
                    _clean_idle_consumer_bg();
                } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(60)));
            },
            &_clean_idle_consumer_thread));
    return Status::OK();
}

void DataConsumerPool::_clean_idle_consumer_bg() {
    const static int32_t max_idle_time_second = 600;

    std::unique_lock<std::mutex> l(_lock);
    time_t now = time(nullptr);

    auto iter = std::begin(_pool);
    while (iter != std::end(_pool)) {
        if (difftime(now, (*iter)->last_visit_time()) >= max_idle_time_second) {
            LOG(INFO) << "remove data consumer " << (*iter)->id()
                      << ", since it last visit: " << (*iter)->last_visit_time()
                      << ", now: " << now;
            iter = _pool.erase(iter);
        } else {
            ++iter;
        }
    }
}

} // end namespace doris
