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
#include <mutex>
#include <unordered_map>

#include "librdkafka/rdkafkacpp.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/blocking_queue.hpp"
#include "util/uid_util.h"

namespace doris {

class KafkaConsumerPipe;
class Status;
class StreamLoadPipe;

class DataConsumer {
public:
    DataConsumer(StreamLoadContext* ctx)
            : _id(UniqueId::gen_uid()),
              _grp_id(UniqueId::gen_uid()),
              _has_grp(false),
              _init(false),
              _cancelled(false),
              _last_visit_time(0) {}

    virtual ~DataConsumer() {}

    // init the consumer with the given parameters
    virtual Status init(StreamLoadContext* ctx) = 0;
    // start consuming
    virtual Status consume(StreamLoadContext* ctx) = 0;
    // cancel the consuming process.
    // if the consumer is not initialized, or the consuming
    // process is already finished, call cancel() will
    // return ERROR
    virtual Status cancel(StreamLoadContext* ctx) = 0;
    // reset the data consumer before being reused
    virtual Status reset() = 0;
    // return true the if the consumer match the need
    virtual bool match(StreamLoadContext* ctx) = 0;

    const UniqueId& id() { return _id; }
    time_t last_visit_time() { return _last_visit_time; }
    void set_grp(const UniqueId& grp_id) {
        _grp_id = grp_id;
        _has_grp = true;
    }

protected:
    UniqueId _id;
    UniqueId _grp_id;
    bool _has_grp;

    // lock to protect the following bools
    std::mutex _lock;
    bool _init;
    bool _cancelled;
    time_t _last_visit_time;
};

class PIntegerPair;
class KafkaEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event& event) {
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
                      << event.broker_name() << " id " << (int)event.broker_id();
            break;

        default:
            LOG(INFO) << "kafka event: " << event.type()
                      << ", err: " << RdKafka::err2str(event.err()) << ", event: " << event.str();
            break;
        }
    }
};

class KafkaDataConsumer : public DataConsumer {
public:
    KafkaDataConsumer(StreamLoadContext* ctx)
            : DataConsumer(ctx),
              _brokers(ctx->kafka_info->brokers),
              _topic(ctx->kafka_info->topic) {}

    virtual ~KafkaDataConsumer() {
        VLOG_NOTICE << "deconstruct consumer";
        if (_k_consumer) {
            _k_consumer->close();
            delete _k_consumer;
            _k_consumer = nullptr;
        }
    }

    virtual Status init(StreamLoadContext* ctx) override;
    // TODO(cmy): currently do not implement single consumer start method, using group_consume
    virtual Status consume(StreamLoadContext* ctx) override { return Status::OK(); }
    virtual Status cancel(StreamLoadContext* ctx) override;
    // reassign partition topics
    virtual Status reset() override;
    virtual bool match(StreamLoadContext* ctx) override;
    // commit kafka offset
    Status commit(std::vector<RdKafka::TopicPartition*>& offset);

    Status assign_topic_partitions(const std::map<int32_t, int64_t>& begin_partition_offset,
                                   const std::string& topic, StreamLoadContext* ctx);

    // start the consumer and put msgs to queue
    Status group_consume(BlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms);

    // get the partitions ids of the topic
    Status get_partition_meta(std::vector<int32_t>* partition_ids);
    // get offsets for times
    Status get_offsets_for_times(const std::vector<PIntegerPair>& times,
            std::vector<PIntegerPair>* offsets);
    // get latest offsets for partitions
    Status get_latest_offsets_for_partitions(const std::vector<int32_t>& partition_ids,
            std::vector<PIntegerPair>* offsets);

private:
    std::string _brokers;
    std::string _topic;
    std::unordered_map<std::string, std::string> _custom_properties;

    KafkaEventCb _k_event_cb;
    RdKafka::KafkaConsumer* _k_consumer = nullptr;
    std::shared_ptr<KafkaConsumerPipe> _k_consumer_pipe;
};

} // end namespace doris
