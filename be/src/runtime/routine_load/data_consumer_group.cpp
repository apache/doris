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

#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>

#include <map>
#include <ostream>
#include <string>
#include <utility>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "common/logging.h"
#include "librdkafka/rdkafkacpp.h"
#include "runtime/routine_load/data_consumer.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/stopwatch.hpp"


namespace doris {

Status KafkaDataConsumerGroup::assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx) {
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
        RETURN_IF_ERROR(
                std::static_pointer_cast<KafkaDataConsumer>(_consumers[i])
                        ->assign_topic_partitions(divide_parts[i], ctx->kafka_info->topic, ctx));
    }

    return Status::OK();
}

KafkaDataConsumerGroup::~KafkaDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
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

Status KafkaDataConsumerGroup::start_all(std::shared_ptr<StreamLoadContext> ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer(std::bind<void>(
                    &KafkaDataConsumerGroup::actual_consume, this, consumer, &_queue,
                    ctx->max_interval_s * 1000, [this, &result_st](const Status& st) {
                        std::unique_lock<std::mutex> lock(_mutex);
                        _counter--;
                        VLOG_CRITICAL << "group counter is: " << _counter << ", grp: " << _grp_id;
                        if (_counter == 0) {
                            _queue.shutdown();
                            LOG(INFO) << "all consumers are finished. shutdown queue. group id: "
                                      << _grp_id;
                        }
                        if (result_st.ok() && !st.ok()) {
                            result_st = st;
                        }
                    }))) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id()
                         << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG_CRITICAL << "submit a data consumer: " << consumer->id()
                          << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t left_rows = ctx->max_batch_rows;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<io::KafkaConsumerPipe> kafka_pipe =
            std::static_pointer_cast<io::KafkaConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch rows: " << left_rows << ", batch size: " << left_bytes << ". "
              << ctx->brief();

    // copy one
    std::map<int32_t, int64_t> cmt_offset = ctx->kafka_info->cmt_offset;

    //improve performance
    Status (io::KafkaConsumerPipe::*append_data)(const char* data, size_t size);
    if (ctx->format == TFileFormatType::FORMAT_JSON) {
        append_data = &io::KafkaConsumerPipe::append_json;
    } else {
        append_data = &io::KafkaConsumerPipe::append_with_line_delimiter;
    }

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_rows <= 0 || left_bytes <= 0) {
            _rows = ctx->max_batch_rows - left_rows;
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << _rows
                      << ", received bytes=" << ctx->max_batch_size - left_bytes << ", eos: " << eos
                      << ", left_time: " << left_time << ", left_rows: " << left_rows
                      << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000 << ", "
                      << ctx->brief();

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                static_cast<void>(consumer->cancel(ctx));
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();
            if (!result_st.ok()) {
                kafka_pipe->cancel(result_st.to_string());
                return result_st;
            }
            static_cast<void>(kafka_pipe->finish());
            ctx->kafka_info->cmt_offset = std::move(cmt_offset);
            ctx->receive_bytes = ctx->max_batch_size - left_bytes;
            return Status::OK();
        }

        RdKafka::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            VLOG_NOTICE << "get kafka message"
                        << ", partition: " << msg->partition() << ", offset: " << msg->offset()
                        << ", len: " << msg->len();

            Status st = (kafka_pipe.get()->*append_data)(static_cast<const char*>(msg->payload()),
                                                         static_cast<size_t>(msg->len()));
            if (st.ok()) {
                left_rows--;
                left_bytes -= msg->len();
                cmt_offset[msg->partition()] = msg->offset();
                VLOG_NOTICE << "consume partition[" << msg->partition() << " - " << msg->offset()
                            << "]";
            } else {
                // failed to append this msg, we must stop
                LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id;
                eos = true;
                {
                    std::unique_lock<std::mutex> lock(_mutex);
                    if (result_st.ok()) {
                        result_st = st;
                    }
                }
            }
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void KafkaDataConsumerGroup::actual_consume(std::shared_ptr<DataConsumer> consumer,
                                            BlockingQueue<RdKafka::Message*>* queue,
                                            int64_t max_running_time_ms, ConsumeFinishCallback cb) {
    Status st = std::static_pointer_cast<KafkaDataConsumer>(consumer)->group_consume(
            queue, max_running_time_ms);
    cb(st);
}

Status PulsarDataConsumerGroup::assign_topic_partitions(std::shared_ptr<StreamLoadContext> ctx) {
    DCHECK(ctx->pulsar_info);
    DCHECK(_consumers.size() >= 1);
    // Cumulative acknowledgement when consuming partitioned topics is not supported by pulsar
    DCHECK(_consumers.size() == ctx->pulsar_info->partitions.size());

    // assign partition to consumers
    int consumer_size = _consumers.size();
    for (int i = 0; i < consumer_size; ++i) {
        auto iter = ctx->pulsar_info->initial_positions.find(ctx->pulsar_info->partitions[i]);
        if (iter != ctx->pulsar_info->initial_positions.end()) {
            RETURN_IF_ERROR(std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])
                                    ->assign_partition(ctx->pulsar_info->partitions[i], ctx, iter->second));
        } else {
            RETURN_IF_ERROR(std::static_pointer_cast<PulsarDataConsumer>(_consumers[i])
                                    ->assign_partition(ctx->pulsar_info->partitions[i], ctx));
        }
    }

    return Status::OK();
}

PulsarDataConsumerGroup::~PulsarDataConsumerGroup() {
    // clean the msgs left in queue
    _queue.shutdown();
    while (true) {
        pulsar::Message* msg;
        if (_queue.blocking_get(&msg)) {
            delete msg;
            msg = nullptr;
        } else {
            break;
        }
    }
    DCHECK(_queue.get_size() == 0);
}

Status PulsarDataConsumerGroup::start_all(std::shared_ptr<StreamLoadContext> ctx) {
    Status result_st = Status::OK();
    // start all consumers
    for (auto& consumer : _consumers) {
        if (!_thread_pool.offer([this, consumer, capture0 = &_queue, capture1 = ctx->max_interval_s * 1000,
                capture2 = [this, &result_st](const Status& st) {
                 std::unique_lock<std::mutex> lock(_mutex);
                 _counter--;
                 VLOG(1) << "group counter is: " << _counter << ", grp: " << _grp_id;
                 if (_counter == 0) {
                     _queue.shutdown();
                     LOG(INFO)
                             << "all consumers are finished. shutdown queue. group id: " << _grp_id;
                 }
                 if (result_st.ok() && !st.ok()) {
                     result_st = st;
                 }
                }] { actual_consume(consumer, capture0, capture1, capture2); })) {
            LOG(WARNING) << "failed to submit data consumer: " << consumer->id() << ", group id: " << _grp_id;
            return Status::InternalError("failed to submit data consumer");
        } else {
            VLOG(1) << "submit a data consumer: " << consumer->id() << ", group id: " << _grp_id;
        }
    }

    // consuming from queue and put data to stream load pipe
    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t received_rows = 0;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<io::PulsarConsumerPipe> pulsar_pipe = std::static_pointer_cast<io::PulsarConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer group: " << _grp_id << ". max time(ms): " << left_time
              << ", batch size: " << left_bytes << ". " << ctx->brief();

    // copy one
    std::map<std::string, pulsar::MessageId> ack_offset = ctx->pulsar_info->ack_offset;

    //improve performance
    Status (io::PulsarConsumerPipe::*append_data)(const char* data, size_t size);
    if (ctx->format == TFileFormatType::FORMAT_JSON) {
        append_data = &io::PulsarConsumerPipe::append_json;
    } else {
        append_data = &io::PulsarConsumerPipe::append_with_line_delimiter;
    }

    MonotonicStopWatch watch;
    watch.start();
    bool eos = false;
    while (true) {
        if (eos || left_time <= 0 || left_bytes <= 0) {
            LOG(INFO) << "consumer group done: " << _grp_id
                      << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                      << ", received rows=" << received_rows << ", received bytes=" << ctx->max_batch_size - left_bytes
                      << ", eos: " << eos << ", left_time: " << left_time << ", left_bytes: " << left_bytes
                      << ", blocking get time(us): " << _queue.total_get_wait_time() / 1000
                      << ", blocking put time(us): " << _queue.total_put_wait_time() / 1000;

            // shutdown queue
            _queue.shutdown();
            // cancel all consumers
            for (auto& consumer : _consumers) {
                static_cast<void>(consumer->cancel(ctx));
            }

            // waiting all threads finished
            _thread_pool.shutdown();
            _thread_pool.join();

            if (!result_st.ok()) {
                // some consumers encounter errors, cancel this task
                pulsar_pipe->cancel(result_st.to_string());
                return result_st;
            }

            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, we have to cancel it, because
                // we do not allow finishing stream load pipe without data
                pulsar_pipe->cancel("Cancelled");
                return Status::InternalError("Cancelled");
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                static_cast<void>(pulsar_pipe->finish());
                ctx->pulsar_info->ack_offset = std::move(ack_offset);
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                get_backlog_nums(ctx);
                acknowledge_cumulative(ctx);
                return Status::OK();
            }
        }

        pulsar::Message* msg;
        bool res = _queue.blocking_get(&msg);
        if (res) {
            std::string partition = msg->getTopicName();
            pulsar::MessageId msg_id = msg->getMessageId();
            std::size_t len = msg->getLength();

            VLOG(3) << "get pulsar message"
                    << ", partition: " << partition << ", message id: " << msg_id << ", len: " << len;

            //filter invalid prefix of json
            const char* filter_data = filter_invalid_prefix_of_json(static_cast<const char*>(msg->getData()), len);
            std::vector<const char*> rows = convert_rows(filter_data);
            size_t rows_size = rows.size();
            bool append_all = true;
            for (const char* row : rows) {
                size_t  row_len = len_of_actual_data(row);
                if (rows_size > 1) {
                    LOG(INFO) << "get pulsar message: " << std::string(row, row_len)
                              << ", partition: " << partition << ", message id: " << msg_id
                              << ", len: " << len << ", filter_len: " << row_len;
                }
                // append filtered data
                Status st = (pulsar_pipe.get()->*append_data)(row, row_len);
                if (!st.ok()) {
                    append_all = false;
                    // failed to append this msg, we must stop
                    LOG(WARNING) << "failed to append msg to pipe. grp: " << _grp_id << ", errmsg=" << st.to_string();
                    eos = true;
                    {
                        std::unique_lock<std::mutex> lock(_mutex);
                        if (result_st.ok()) {
                            result_st = st;
                        }
                    }
                    break;
                }
            }
            if (append_all) {
                received_rows++;
                // len of receive origin message from pulsar
                left_bytes -= len;
                ack_offset[partition] = msg_id;
                VLOG(3) << "consume partition" << partition << " - " << msg_id;
            }
            delete msg;
        } else {
            // queue is empty and shutdown
            eos = true;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK();
}

void PulsarDataConsumerGroup::actual_consume(const std::shared_ptr<DataConsumer>& consumer,
                                             BlockingQueue<pulsar::Message*>* queue, int64_t max_running_time_ms,
                                             const ConsumeFinishCallback& cb) {
    Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->group_consume(queue, max_running_time_ms);
    cb(st);
}

void PulsarDataConsumerGroup::get_backlog_nums(std::shared_ptr<StreamLoadContext> ctx) {
    for (auto& consumer : _consumers) {
        // get backlog num
        int64_t backlog_num;
        Status st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition_backlog(&backlog_num);
        if (!st.ok()) {
            LOG(WARNING) << st.to_string();
        } else {
            ctx->pulsar_info
                    ->partition_backlog[std::static_pointer_cast<PulsarDataConsumer>(consumer)->get_partition()] =
                    backlog_num;
        }
    }
}

void PulsarDataConsumerGroup::acknowledge_cumulative(std::shared_ptr<StreamLoadContext> ctx) {
    for (auto& kv : ctx->pulsar_info->ack_offset) {
        LOG(INFO) << "start do ack of kv :" << kv;
        for (auto& consumer : _consumers) {
            Status st;
            // do ack
            st = std::static_pointer_cast<PulsarDataConsumer>(consumer)->acknowledge_cumulative(kv.second);
            if (!st.ok()) {
                // Pulsar Offset Acknowledgement is idempotent, Failure should not block the normal process
                // So just print a warning
                LOG(WARNING) << "consumer id : " << consumer->id() << "can not ack" << st;
            }
        }
        LOG(INFO) << "finish do ack of kv :" << kv;
    }
}

const char* PulsarDataConsumerGroup::filter_invalid_prefix_of_json(const char* data, std::size_t size) {
    // first index of '{'
    int first_left_curly_bracket_index  = -1;
    for (int i = 0; i < size; ++i) {
        if (first_left_curly_bracket_index == -1 && data[i] == '{') {
            first_left_curly_bracket_index = i;
            if ( i+1 < size && data[i+1] == '{') {
                first_left_curly_bracket_index = i + 1;
            }
            break;
        }
    }
    if (first_left_curly_bracket_index >= 0) {
        return data + first_left_curly_bracket_index;
    } else {
        return data;
    }
}

size_t PulsarDataConsumerGroup::len_of_actual_data(const char* data) {
    size_t length = 0;
    while (data[length] != '\0') {
        ++length;
    }
    return length;
}

std::vector<const char*> PulsarDataConsumerGroup::convert_rows(const char* data) {
    std::vector<const char*> targets;
    rapidjson::Document source;
    if(!source.Parse(data).HasParseError()) {
        if (source.HasMember("events") && source["events"].IsArray()) {
            const rapidjson::Value& array = source["events"];
            size_t len = array.Size();
            for(size_t i = 0; i < len; i++) {
                rapidjson::Document destination;
                destination.SetObject();
                rapidjson::Value& object = const_cast<rapidjson::Value&>(array[i]);
                rapidjson::Value eventName("event", destination.GetAllocator());
                destination.AddMember(eventName, object, destination.GetAllocator());
                for (auto& member : source.GetObject()) {
                    const char* key = member.name.GetString();
                    if (std::strcmp(key, "events") != 0) {
                        rapidjson::Value keyName(key, destination.GetAllocator());
                        rapidjson::Value& sourceValue = source[key];
                        destination.AddMember(keyName, sourceValue, destination.GetAllocator());
                    }
                }
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                destination.Accept(writer);
                targets.push_back(buffer.GetString());
            }
        } else {
            targets.push_back(data);
        }
    } else {
        targets.push_back(data);
    }
    return targets;
}


} // namespace doris
