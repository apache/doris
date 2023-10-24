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

#include <stdint.h>

#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "common/status.h"
#include "io/fs/kafka_consumer_pipe.h"
#include "runtime/routine_load/data_consumer.h"
#include "util/blocking_queue.hpp"
#include "util/uid_util.h"
#include "util/work_thread_pool.hpp"

namespace RdKafka {
class Message;
} // namespace RdKafka

namespace doris {
class StreamLoadContext;

// data consumer group saves a group of data consumers.
// These data consumers share the same stream load pipe.
// This class is not thread safe.
class DataConsumerGroup {
public:
    typedef std::function<void(const Status&)> ConsumeFinishCallback;

    DataConsumerGroup()
            : _grp_id(UniqueId::gen_uid()),
              _thread_pool(3, 10, "data_consumer"),
              _counter(0),
              _rows(0) {}

    virtual ~DataConsumerGroup() { _consumers.clear(); }

    const UniqueId& grp_id() { return _grp_id; }

    const std::vector<std::shared_ptr<DataConsumer>>& consumers() { return _consumers; }

    void add_consumer(std::shared_ptr<DataConsumer> consumer) {
        consumer->set_grp(_grp_id);
        _consumers.push_back(consumer);
        ++_counter;
    }

    int64_t get_consumer_rows() const { return _rows; }

    void set_consumer_rows(int64_t rows) { _rows = rows; }

    // start all consumers
    virtual Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                             std::shared_ptr<io::KafkaConsumerPipe> kafka_pipe) {
        return Status::OK();
    }

protected:
    UniqueId _grp_id;
    std::vector<std::shared_ptr<DataConsumer>> _consumers;
    // thread pool to run each consumer in multi thread
    PriorityThreadPool _thread_pool;
    // mutex to protect counter.
    // the counter is init as the number of consumers.
    // once a consumer is done, decrease the counter.
    // when the counter becomes zero, shutdown the queue to finish
    std::mutex _mutex;
    int _counter;
    // received total rows
    int64_t _rows;
};

// for kafka
class KafkaDataConsumerGroup : public DataConsumerGroup {
public:
    KafkaDataConsumerGroup() : DataConsumerGroup(), _queue(500) {}

    virtual ~KafkaDataConsumerGroup();

    Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                     std::shared_ptr<io::KafkaConsumerPipe> kafka_pipe) override;
    // assign topic partitions to all consumers equally
    Status assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx);

private:
    // start a single consumer
    void actual_consume(std::shared_ptr<DataConsumer> consumer,
                        BlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms,
                        ConsumeFinishCallback cb);

private:
    // blocking queue to receive msgs from all consumers
    BlockingQueue<RdKafka::Message*> _queue;
};

} // end namespace doris
