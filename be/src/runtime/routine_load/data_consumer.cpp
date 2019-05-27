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
    RETURN_IF_ERROR(set_conf("api.version.request", "true"));
    RETURN_IF_ERROR(set_conf("api.version.fallback.ms", "0"));

    for(auto item : ctx->kafka_info->properties) {
        std::size_t found = item.first.find("property.");
        if (found != std::string::npos) {
            RETURN_IF_ERROR(set_conf(item.first.substr(found + 9), item.second));
        }
    }

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

Status KafkaDataConsumer::assign_topic_partitions(
        const std::map<int32_t, int64_t>& begin_partition_offset,
        const std::string& topic,
        StreamLoadContext* ctx) {

    DCHECK(_k_consumer);
    // create TopicPartitions
    std::stringstream ss;
    std::vector<RdKafka::TopicPartition*> topic_partitions;
    for (auto& entry : begin_partition_offset) {
        RdKafka::TopicPartition* tp1 = RdKafka::TopicPartition::create(
                topic, entry.first, entry.second);
        topic_partitions.push_back(tp1);
        ss << "[" << entry.first << ": " << entry.second << "] ";
    }

    LOG(INFO) << "consumer: " << _id << ", grp: " << _grp_id
            << " assign topic partitions: " << topic << ", " << ss.str();

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

Status KafkaDataConsumer::group_consume(
        BlockingQueue<RdKafka::Message*>* queue,
        int64_t max_running_time_ms) {
    _last_visit_time = time(nullptr);
    int64_t left_time = max_running_time_ms;
    LOG(INFO) << "start kafka consumer: " << _id << ", grp: " << _grp_id
            << ", max running time(ms): " << left_time;

    int64_t received_rows = 0;
    int64_t put_rows = 0;
    Status st = Status::OK;
    MonotonicStopWatch consumer_watch;
    MonotonicStopWatch watch;
    watch.start();
    while (true) {
        {
            std::unique_lock<std::mutex> l(_lock);
            if (_cancelled) { break; }
        }

        if (left_time <= 0) { break; }

        bool done = false;
        // consume 1 message at a time
        consumer_watch.start();
        std::unique_ptr<RdKafka::Message> msg(_k_consumer->consume(1000 /* timeout, ms */));
        consumer_watch.stop();
        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                if (!queue->blocking_put(msg.get())) {
                    // queue is shutdown
                    done = true;
                } else {
                    ++put_rows;
                    msg.release(); // release the ownership, msg will be deleted after being processed
                }
                ++received_rows;
                break;
            case RdKafka::ERR__TIMED_OUT:
                // leave the status as OK, because this may happend
                // if there is no data in kafka.
                LOG(WARNING) << "kafka consume timeout: " << _id;
                break;
            default:
                LOG(WARNING) << "kafka consume failed: " << _id
                        << ", msg: " << msg->errstr();
                done = true;
                st = Status(msg->errstr());
                break;
        }

        left_time = max_running_time_ms - watch.elapsed_time() / 1000 / 1000;
        if (done) { break; }
    }

    LOG(INFO) << "kafka conumer done: " << _id << ", grp: " << _grp_id
            << ". cancelled: " << _cancelled
            << ", left time(ms): " << left_time
            << ", total cost(ms): " << watch.elapsed_time() / 1000 / 1000
            << ", consume cost(ms): " << consumer_watch.elapsed_time() / 1000 / 1000
            << ", received rows: " << received_rows
            << ", put rows: " << put_rows;

    return st;
}

Status KafkaDataConsumer::cancel(StreamLoadContext* ctx) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status("consumer is not initialized");
    }

    _cancelled = true;
    LOG(INFO) << "kafka consumer cancelled. " << _id;
    return Status::OK;
}

Status KafkaDataConsumer::reset() {
    std::unique_lock<std::mutex> l(_lock);
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
