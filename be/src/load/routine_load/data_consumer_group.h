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

#include "common/cast_set.h"
#include "common/status.h"
#include "load/routine_load/data_consumer.h"
#include "util/blocking_queue.hpp"
#include "util/uid_util.h"
#include "util/work_thread_pool.hpp"

namespace RdKafka {
class Message;
} // namespace RdKafka

namespace doris {
#include "common/compile_check_begin.h"
class StreamLoadContext;

// data consumer group saves a group of data consumers.
// These data consumers share the same stream load pipe.
// This class is not thread safe.
class DataConsumerGroup {
public:
    typedef std::function<void(const Status&)> ConsumeFinishCallback;

    DataConsumerGroup(size_t consumer_num)
            : _grp_id(UniqueId::gen_uid()),
              _thread_pool(doris::cast_set<uint32_t>(consumer_num),
                           doris::cast_set<uint32_t>(consumer_num), "data_consumer"),
              _counter(0) {}

    virtual ~DataConsumerGroup() { _consumers.clear(); }

    const UniqueId& grp_id() { return _grp_id; }

    const std::vector<std::shared_ptr<DataConsumer>>& consumers() { return _consumers; }

    void add_consumer(std::shared_ptr<DataConsumer> consumer) {
        consumer->set_grp(_grp_id);
        _consumers.push_back(consumer);
        ++_counter;
    }

    // start all consumers
    virtual Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                             std::shared_ptr<io::StreamLoadPipe> pipe) {
        return Status::OK();
    }

protected:
    // Submit all consumers to thread pool.
    // consume_fn: wraps actual_consume per consumer.
    // shutdown_fn: called when last consumer finishes (shuts down queue).
    // Returns false if any submission fails.
    bool _submit_all_consumers(
            std::function<void(std::shared_ptr<DataConsumer>, ConsumeFinishCallback)> consume_fn,
            std::function<void()> shutdown_fn, Status& result_st);

    // Shared consumption loop skeleton. Calls _dequeue_and_process per iteration.
    Status _run_consume_loop(std::shared_ptr<StreamLoadContext> ctx,
                             std::shared_ptr<io::StreamLoadPipe> pipe, Status& result_st);

    // Dequeue one item and append to pipe. Update left_rows/left_bytes.
    // Returns false → queue empty/shutdown (eos). Returns true → continue.
    virtual bool _dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows,
                                      int64_t& left_bytes, Status& result_st) = 0;

    // Shutdown the subclass queue. Called at loop exit.
    virtual void _shutdown_queue() = 0;

    // Called after successful finish. Override to collect post-consume state.
    virtual void _on_finish(std::shared_ptr<StreamLoadContext> ctx) {}

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
};

// for kafka
class KafkaDataConsumerGroup : public DataConsumerGroup {
public:
    KafkaDataConsumerGroup(size_t consumer_num) : DataConsumerGroup(consumer_num), _queue(500) {}

    ~KafkaDataConsumerGroup() override;

    Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                     std::shared_ptr<io::StreamLoadPipe> pipe) override;
    // assign topic partitions to all consumers equally
    Status assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx);

    // start a single consumer
    void actual_consume(std::shared_ptr<DataConsumer> consumer,
                        BlockingQueue<RdKafka::Message*>* queue, int64_t max_running_time_ms,
                        ConsumeFinishCallback cb);

private:
    bool _dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows, int64_t& left_bytes,
                              Status& result_st) override;
    void _shutdown_queue() override { _queue.shutdown(); }
    void _on_finish(std::shared_ptr<StreamLoadContext> ctx) override;

    BlockingQueue<RdKafka::Message*> _queue;
    std::map<int32_t, int64_t> _cmt_offset;
    TFileFormatType::type _format;
};

// for kinesis
class KinesisDataConsumerGroup : public DataConsumerGroup {
public:
    KinesisDataConsumerGroup(size_t consumer_num) : DataConsumerGroup(consumer_num), _queue(500) {}

    ~KinesisDataConsumerGroup() override;

    Status start_all(std::shared_ptr<StreamLoadContext> ctx,
                     std::shared_ptr<io::StreamLoadPipe> pipe) override;

    Status assign_stream_shards(std::shared_ptr<StreamLoadContext> ctx);

private:
    void actual_consume(std::shared_ptr<DataConsumer> consumer,
                        BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>>* queue,
                        int64_t max_running_time_ms, ConsumeFinishCallback cb);

    bool _dequeue_and_process(io::StreamLoadPipe* pipe, int64_t& left_rows, int64_t& left_bytes,
                              Status& result_st) override;
    void _shutdown_queue() override { _queue.shutdown(); }
    void _on_finish(std::shared_ptr<StreamLoadContext> ctx) override;

    BlockingQueue<std::shared_ptr<Aws::Kinesis::Model::Record>> _queue;
    TFileFormatType::type _format;
};

#include "common/compile_check_end.h"

} // end namespace doris
