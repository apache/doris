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

namespace doris {

Status DataConsumerPool::get_consumer(
        StreamLoadContext* ctx,
        std::shared_ptr<DataConsumer>* ret) {

    std::unique_lock<std::mutex> l(_lock);

    // check if there is an available consumer.
    // if has, return it
    for (auto& c : _pool) {
        if (c->match(ctx)) {
            VLOG(3) << "get an available data consumer from pool: " << c->id();
            c->reset();
            *ret = c;
            return Status::OK; 
        }
    }

    // no available consumer, create a new one
    std::shared_ptr<DataConsumer> consumer;
    switch (ctx->load_src_type) {
        case TLoadSourceType::KAFKA:
            consumer = std::make_shared<KafkaDataConsumer>(ctx);
            break;
        default:
            std::stringstream ss;
            ss << "unknown routine load task type: " << ctx->load_type;
            return Status(ss.str());
    }
    
    // init the consumer
    RETURN_IF_ERROR(consumer->init(ctx));

    VLOG(3) << "create new data consumer: " << consumer->id();
    *ret = consumer;
    return Status::OK;
}

void DataConsumerPool::return_consumer(std::shared_ptr<DataConsumer> consumer) {
    std::unique_lock<std::mutex> l(_lock);

    if (_pool.size() == _max_pool_size) {
       VLOG(3) << "data consumer pool is full: " << _pool.size()
                << "-" << _max_pool_size << ", discard the returned consumer: "
                << consumer->id();
        return;
    }

    consumer->reset();
    _pool.push_back(consumer);
    VLOG(3) << "return the data consumer: " << consumer->id()
            << ", current pool size: " << _pool.size();
    return;
}

} // end namespace doris
