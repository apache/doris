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


#include "runtime/routine_load/data_consumer_group.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"

namespace doris {

Status KafkaDataConsumerGroup::assign_topic_partitions(StreamLoadContext* ctx) {
    DCHECK(ctx->kafka_info);
    DCHECK(_consumers.size() >= 1);

    // divide partitions
    int consumer_size = _consumers.size();
    std::vector<std::map<int32_t, int64_t>> divide_parts(consumer_size);
    int i = 0;
    for (auto& kv : ctx->kafka_info->begin_offset) {
        int idx = i % consumer_size;
        divide_parts[idx].emplace(kv.first, kv.second);
        i++;
    }

    // assign partitions to consumers equally
    for (int i = 0; i < consumer_size; ++i) {
        RETURN_IF_ERROR(std::static_pointer_cast<KafkaDataConsumer>(_consumers[i])->assign_topic_partitions(
                divide_parts[i], ctx->kafka_info->topic, ctx));
    }

    return Status::OK;
}

KafkaDataConsumerGroup::~KafkaDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while(true) {
        RdKafka::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status KafkaDataConsumerGroup::start_all(StreamLoadContext* ctx) {
    Status result_st = Status::OK;
    // start all consumers
    for(auto& consumer : _consumers) {
        if (!_thread_pool.offer(
            boost::bind<void>(&KafkaDataConsumerGroup::actual_consume, this, consumer, &_queue, ctx->max_interval_s * 1000,
            [this, &result_st] (const Status& st) { 
                std::unique_lock<std::mutex> lock(_mutex);
                _counter--;
                VLOG(1) << "group counter is: " << _counter << ", grp: " << _grp_id;
                if (_counter == 0) {
                    _queue.shutdown();
                    LOG(INFO) << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                } 
                if (result_st.ok() && !st.ok()) {
                    result_st = st;
                }
            }))) {

            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status("failed to submit data consumer");
        } else {
            VLOG(1) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t left_rows = ctx->max_batch_rows;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<KafkaConsumerPipe> kafka_pipe = std::static_pointer_cast<KafkaConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id
        << ". max time(ms): " << left_time
        << ", batch rows: " << left_rows
        << ", batch size: " << left_bytes
        << ". " << ctx->brief();

    // copy one
    std::map<int32_t, int64_t> cmt_offset = ctx->kafka_info->cmt_offset;

    MonotonicStopWatch watch;
    watch.start();
    Status st;
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_rows <= 0 || left_bytes <=0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                    << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                    << ", received rows=" << ctx->max_batch_rows - left_rows
                    << ", received bytes=" << ctx->max_batch_size - left_bytes
                    << ", eos: " << eos
                    << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                    << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;
            
            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) { consumer->cancel(ctx); }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some of consumers encounter errors, cancel this task
                return result_st;
            }

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data
                kafka_pipe->cancel();
                return Status::CANCELLED;
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                DCHECK(left_rows < ctx->max_batch_rows);
                kafka_pipe->finish();
                ctx->kafka_info->cmt_offset = std::move(cmt_offset);
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                return Status::OK;
            }
        }

        RdKafka::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            VLOG(3) << "get kafka message"
                << ", partition: " << msg->partition()
                << ", offset: " << msg->offset()
                << ", len: " << msg->len();

            st = kafka_pipe->append_with_line_delimiter(
                    static_cast<const char *>(msg->payload()),
                    static_cast<size_t>(msg->len()));
            if (st.ok()) {
                left_rows--;
                left_bytes -= msg->len();
                cmt_offset[msg->partition()] = msg->offset();
                VLOG(3) << "consume partition[" << msg->partition()
                    << " - " << msg->offset() << "]";
            } else {
                // failed to append this msg, we must stop
                LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id;
                eos = true;
            }
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;   
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK;
}

void KafkaDataConsumerGroup::actual_consume(
        std::shared_ptr<DataConsumer> consumer,
        BlockingQueue<RdKafka::Message*>* queue,
        int64_t max_running_time_ms,
        ConsumeFinishCallback cb) {
    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->group_consume(queue, max_running_time_ms);
    cb(st);
}

} // end namespace
