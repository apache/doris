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

#include "runtime/routine_load/data_consumer.h"

#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "service/backend_options.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/uid_util.h"

namespace doris {

// init kafka consumer will only set common configs such as
// brokers, groupid
Status KafkaDataConsumer::init(StreamLoadContext* ctx) {
    std::unique_lock<std::mutex> l(_lock); 
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK;
    }

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    
    // conf has to be deleted finally
    auto conf_deleter = [conf] () { delete conf; };
    DeferOp delete_conf(std::bind<void>(conf_deleter));

    std::stringstream ss;
    ss << BackendOptions::get_localhost() << "_";
    std::string group_id = ss.str() + UniqueId().to_string();
    LOG(INFO) << "init kafka consumer with group id: " << group_id;

    std::string errstr;
    auto set_conf = [&conf, &errstr](const std::string& conf_key, const std::string& conf_val) {
        if (conf->set(conf_key, conf_val, errstr) != RdKafka::Conf::CONF_OK) {
            std::stringstream ss;
            ss << "failed to set '" << conf_key << "'";
            LOG(WARNING) << ss.str();
            return Status(ss.str());
        }
        VLOG(3) << "set " << conf_key << ": " << conf_val;
        return Status::OK;
    };

    RETURN_IF_ERROR(set_conf("metadata.broker.list", ctx->kafka_info->brokers));
    RETURN_IF_ERROR(set_conf("group.id", group_id));
    RETURN_IF_ERROR(set_conf("enable.partition.eof", "false"));
    RETURN_IF_ERROR(set_conf("enable.auto.offset.store", "false"));
    // TODO: set it larger than 0 after we set rd_kafka_conf_set_stats_cb()
    RETURN_IF_ERROR(set_conf("statistics.interval.ms", "0"));
    RETURN_IF_ERROR(set_conf("auto.offset.reset", "error"));

    if (conf->set("event_cb", &_k_event_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::stringstream ss;
        ss << "failed to set 'event_cb'";
        LOG(WARNING) << ss.str();
        return Status(ss.str());
    }

    // create consumer
    _k_consumer = RdKafka::KafkaConsumer::create(conf, errstr); 
    if (!_k_consumer) {
        LOG(WARNING) << "failed to create kafka consumer";
        return Status("failed to create kafka consumer");
    }

    VLOG(3) << "finished to init kafka consumer. " << ctx->brief();

    _init = true;
    return Status::OK;
}

Status KafkaDataConsumer::assign_topic_partitions(StreamLoadContext* ctx) {
    DCHECK(_k_consumer);
    // create TopicPartitions
    std::stringstream ss;
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (auto& entry : ctx->kafka_info->begin_offset) {
        RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(
                ctx->kafka_info->topic, entry.first, entry.second);
        topic_partitions.push_back(tp1);
        ss << "partition[" << entry.first << "-" << entry.second << "] ";
    }

    VLOG(1) << "assign topic partitions: " << ctx->kafka_info->topic
        << ", " << ss.str();

    // delete TopicPartition finally
    auto tp_deleter = [&topic_partitions] () {
            std::for_each(topic_partitions.begin(), topic_partitions.end(),
                    [](RdKafka::TopicPartition* tp1) { delete tp1; });
    };
    DeferOp delete_tp(std::bind<void>(tp_deleter));

    // assign partition
    RdKafka::ErrorCode err = _k_consumer->assign(topic_partitions);
    if (err) {
        LOG(WARNING) << "failed to assign topic partitions: " << ctx->brief(true)
                << ", err: " << RdKafka::err2str(err);
        return Status("failed to assign topic partitions");
    }

    return Status::OK;
}

Status KafkaDataConsumer::start(StreamLoadContext* ctx) {
    {
        std::unique_lock<std::mutex> l(_lock);
        if (!_init) {
            return Status("consumer is not initialized");
        }
    }
    
    _last_visit_time = time(nullptr);

    int64_t left_time = ctx->max_interval_s * 1000;
    int64_t left_rows = ctx->max_batch_rows;
    int64_t left_bytes = ctx->max_batch_size;

    std::shared_ptr<KafkaConsumerPipe> kakfa_pipe = std::static_pointer_cast<KafkaConsumerPipe>(ctx->body_sink);

    LOG(INFO) << "start consumer"
        << ". max time(ms): " << left_time
        << ", batch rows: " << left_rows
        << ", batch size: " << left_bytes
        << ". " << ctx->brief();

    // copy one
    std::map<int32_t, int64_t> cmt_offset = ctx->kafka_info->cmt_offset;
    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();
    Status st;
    while (true) {
        std::unique_lock<std::mutex> l(_lock);
        if (_cancelled) {
            kakfa_pipe ->cancel();
            return Status::CANCELLED;
        }

        if (_finished) {
            kakfa_pipe ->finish();
            ctx->kafka_info->cmt_offset = std::move(cmt_offset); 
            return Status::OK;
        }

        if (left_time <= 0 || left_rows <= 0 || left_bytes <=0) {
            LOG(INFO) << "kafka consume batch done"
                    << ". consume time(ms)=" << ctx->max_interval_s * 1000 - left_time
                    << ", received rows=" << ctx->max_batch_rows - left_rows
                    << ", received bytes=" << ctx->max_batch_size - left_bytes
                    << ", kafka consume time(ms)=" << consumer_watch.elapsed_time() / 1000 / 1000;


            if (left_bytes == ctx->max_batch_size) {
                // nothing to be consumed, cancel it
                // we do not allow finishing stream load pipe without data
                kakfa_pipe->cancel();
                _cancelled = true;
                return Status::CANCELLED;
            } else {
                DCHECK(left_bytes < ctx->max_batch_size);
                DCHECK(left_rows < ctx->max_batch_rows);
                kakfa_pipe->finish();
                ctx->kafka_info->cmt_offset = std::move(cmt_offset); 
                ctx->receive_bytes = ctx->max_batch_size - left_bytes;
                _finished = true;
                return Status::OK;
            }
        }

        // consume 1 message at a time
        consumer_watch.start();
        RdKafka::Message *msg = _k_consumer->consume(1000 /* timeout, ms */);
        consumer_watch.stop();
        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                VLOG(3) << "get kafka message"
                        << ", partition: " << msg->partition()
                        << ", offset: " << msg->offset()
                        << ", len: " << msg->len();

                st = kakfa_pipe ->append_with_line_delimiter(
                        static_cast<const char *>(msg->payload()),
                        static_cast<size_t>(msg->len()));
                if (st.ok()) {
                    left_rows--;
                    left_bytes -= msg->len();
                    cmt_offset[msg->partition()] = msg->offset();
                    VLOG(3) << "consume partition[" << msg->partition()
                            << " - " << msg->offset() << "]";
                }

                break;
            case RdKafka::ERR__TIMED_OUT:
                // leave the status as OK, because this may happend
                // if there is no data in kafka.
                LOG(WARNING) << "kafka consume timeout";
                break;
            default:
                LOG(WARNING) << "kafka consume failed: " << msg->errstr();
                st = Status(msg->errstr());
                break;
        }
        delete msg; 

        if (!st.ok()) {
            kakfa_pipe ->cancel();
            return st;
        }

        left_time = ctx->max_interval_s * 1000 - watch.elapsed_time() / 1000 / 1000;
    }

    return Status::OK;
}

Status KafkaDataConsumer::cancel(StreamLoadContext* ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status("consumer is not initialized");
    }

    if (_finished) {
        return Status("consumer is already finished");
    }

    _cancelled = true;
    return Status::OK;
}

Status KafkaDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
    _finished = false;
    _cancelled = false;
    return Status::OK;
}

// if the kafka brokers and topic are same,
// we considered this consumer as matched, thus can be reused.
bool KafkaDataConsumer::match(StreamLoadContext* ctx) {
    if (ctx->load_src_type != TLoadSourceType::KAFKA) {
        return false;
    }
    if (_brokers != ctx->kafka_info->brokers || _topic != ctx->kafka_info->topic) {
        return false;
    }
    return true;
}

} // end namespace doris
