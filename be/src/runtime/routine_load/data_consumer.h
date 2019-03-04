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

#include <mutex>

#include "librdkafka/rdkafkacpp.h"

#include "runtime/stream_load/stream_load_context.h"

namespace doris {

class KafkaConsumerPipe;
class Status;

class DataConsumer {
public:
    DataConsumer(StreamLoadContext* ctx):
        _ctx(ctx),
        _init(false),
        _finished(false),
        _cancelled(false) {

        _ctx->ref();
    }

    virtual ~DataConsumer() {
        if (_ctx->unref()) {
            delete _ctx;
        }
    }

    // init the consumer with the given parameters
    virtual Status init() = 0;
    
    // start consuming
    virtual Status start() = 0;

    // cancel the consuming process.
    // if the consumer is not initialized, or the consuming
    // process is already finished, call cancel() will
    // return ERROR
    virtual Status cancel() = 0;

protected:
    StreamLoadContext* _ctx;

    // lock to protect the following bools
    std::mutex _lock;
    bool _init;
    bool _finished;
    bool _cancelled;
};

class KafkaDataConsumer : public DataConsumer {
public:
    KafkaDataConsumer(
            StreamLoadContext* ctx,
            std::shared_ptr<KafkaConsumerPipe> kafka_consumer_pipe 
            ):
        DataConsumer(ctx),
        _kafka_consumer_pipe(kafka_consumer_pipe) {
    }

    virtual Status init() override;

    virtual Status start() override;

    virtual Status cancel() override;

    virtual ~KafkaDataConsumer() {
        if (_k_consumer) {
            _k_consumer->close();
            delete _k_consumer;
        }
    }

private:
    std::shared_ptr<KafkaConsumerPipe> _kafka_consumer_pipe;
    RdKafka::KafkaConsumer* _k_consumer = nullptr;
};

class KafkaEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event) {
        switch (event.type()) {
            case RdKafka::Event::EVENT_ERROR:
                LOG(INFO) << "kafka error: " << RdKafka::err2str(event.err())
                          << ", event: " << event.str();
                break;
            case RdKafka::Event::EVENT_STATS:
                LOG(INFO) << "kafka stats: " << event.str();
                break;

            case RdKafka::Event::EVENT_LOG:
                LOG(INFO) << "kafka log-" << event.severity() << "-" << event.fac().c_str()
                          << ", event: " << event.str();
                break;

            case RdKafka::Event::EVENT_THROTTLE:
                LOG(INFO) << "kafka throttled: " << event.throttle_time() << "ms by "
                          <<  event.broker_name() << " id " << (int) event.broker_id();
                break;

            default:
                LOG(INFO) << "kafka event: " << event.type()
                          << ", err: " << RdKafka::err2str(event.err())
                          << ", event: " << event.str();
                break;
        }
    }
};

} // end namespace doris
