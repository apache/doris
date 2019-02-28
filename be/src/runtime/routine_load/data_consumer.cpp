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
#include "runtime/routine_load/kafka_consumer_pipe.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"

namespace doris {

Status KafkaDataConsumer::init() {
    std::unique_lock<std::mutex> l(_lock); 
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK;
    }

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    
    // conf has to be deleted finally
    auto conf_deleter = [conf] () { delete conf; };
    DeferOp delete_conf(std::bind<void>(conf_deleter));

    std::string errstr;
    auto set_conf = [conf, &errstr](const std::string& conf_key, const std::string& conf_val) {
        if (conf->set(conf_key, conf_val, errstr) != RdKafka::Conf::CONF_OK) {
            std::stringstream ss;
            ss << "failed to set '" << conf_key << "'";
            LOG(WARNING) << ss.str();
            return Status(ss.str());
        }
        return Status::OK;
    };

    RETURN_IF_ERROR(set_conf("metadata.broker.list", _ctx->kafka_info->brokers));

    RETURN_IF_ERROR(set_conf("metadata.broker.list", _ctx->kafka_info->brokers));
    RETURN_IF_ERROR(set_conf("group.id", _ctx->kafka_info->group_id));
    RETURN_IF_ERROR(set_conf("client.id", _ctx->kafka_info->client_id));
    RETURN_IF_ERROR(set_conf("enable.partition.eof", "false"));
    RETURN_IF_ERROR(set_conf("enable.auto.offset.store", "false"));
    // TODO: set it larger than 0 after we set rd_kafka_conf_set_stats_cb()
    RETURN_IF_ERROR(set_conf("statistics.interval.ms", "0"));

    // create consumer
    _k_consumer = RdKafka::KafkaConsumer::create(conf, errstr); 
    if (!_k_consumer) {
        LOG(WARNING) << "failed to create kafka consumer";
        return Status("failed to create kafka consumer");
    }

    // create TopicPartitions
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (auto& entry : _ctx->kafka_info->begin_offset) {
        RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(
                _ctx->kafka_info->topic, entry.first, entry.second);
        topic_partitions.push_back(tp1);
    }

    // delete TopicPartition finally
    auto tp_deleter = [&topic_partitions] () {
            std::for_each(topic_partitions.begin(), topic_partitions.end(),
                    [](RdKafka::TopicPartition* tp1) { delete tp1; });
    };
    DeferOp delete_tp(std::bind<void>(tp_deleter));

    // assign partition
    RdKafka::ErrorCode err = _k_consumer->assign(topic_partitions);
    if (err) {
        LOG(WARNING) << "failed to assign topic partitions: " << _ctx->brief(true)
                << ", err: " << RdKafka::err2str(err);
        return Status("failed to assgin topic partitions");
    }

    VLOG(3) << "finished to init kafka consumer. "
            << _ctx->brief(true);

    _init = true;
    return Status::OK;
}

Status KafkaDataConsumer::start() {
    {
        std::unique_lock<std::mutex> l(_lock);
        if (!_init) {
            return Status("consumer is not initialized");
        }
    }
    
    int64_t left_time = _ctx->kafka_info->max_interval_s;
    int64_t left_rows = _ctx->kafka_info->max_batch_rows;
    int64_t left_bytes = _ctx->kafka_info->max_batch_bytes;

    LOG(INFO) << "start consumer"
        << ". interval(s): " << left_time
        << ", bath rows: " << left_rows
        << ", batch size: " << left_bytes
        << ". " << _ctx->brief();

    MonotonicStopWatch watch;
    watch.start();
    Status st;
    while (true) {
        std::unique_lock<std::mutex> l(_lock);
        if (_cancelled) {
            _kafka_consumer_pipe->cancel();
            return Status::CANCELLED;
        }

        if (_finished) {
            _kafka_consumer_pipe->finish();
            return Status::OK;
        }

        if (left_time <= 0 || left_rows <= 0 || left_bytes <=0) {
            VLOG(3) << "kafka consume batch finished"
                    << ". left time=" << left_time
                    << ", left rows=" << left_rows
                    << ", left bytes=" << left_bytes; 
            _kafka_consumer_pipe->finish();
            _finished = true;
            return Status::OK;
        }

        // consume 1 message at a time
        RdKafka::Message *msg = _k_consumer->consume(1000 /* timeout, ms */);
        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                LOG(INFO) << "get kafka message"
                        << ", partition: " << msg->partition()
                        << ", offset: " << msg->offset()
                        << ", len: " << msg->len();

                st = _kafka_consumer_pipe->append_with_line_delimiter(
                        static_cast<const char *>(msg->payload()),
                        static_cast<size_t>(msg->len()));
                if (st.ok()) {
                    left_rows--;
                    left_bytes -= msg->len();
                    _ctx->kafka_info->cmt_offset[msg->partition()] = msg->offset();
                    VLOG(3) << "consume partition[ " << msg->partition()
                            << " - " << msg->offset();
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
            _kafka_consumer_pipe->cancel();
            return st;
        }

        left_time = _ctx->kafka_info->max_interval_s - watch.elapsed_time() / 1000 / 1000 / 1000; 
    }

    return Status::OK;
}

Status KafkaDataConsumer::cancel() {
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

} // end namespace doris
