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

#include "runtime/routine_load/kafka_consumer.h"

#include "runtime/routine_load/"
#include "librdkafka/rdkafkacpp.h"

namespace doris {

Status KafkaConsumer::init() {
    std::unique_lock<std::mutex> l(_lock); 
    if (_init) {
        // this consumer has already been initialized.
        return Status::OK;
    }

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    
    // conf has to be deleted finally
    auto conf_deleter = [] (RdKafka::Conf *conf) { delete conf; };
    DeferOp close_dir(std::bind<void>(&conf_deleter, conf));

    std::string errstr;

#define SET_KAFKA_CONF(conf_key, conf_val) \
    if (conf->set(#conf_key, conf_val, errstr) != RdKafka::Conf::CONF_OK) { \
        std::stringstream ss; \
        ss << "failed to set '" << #conf_key << "'"; \
        LOG(WARNING) << ss.str(); \
        return Status(ss.str()); \
    }

    SET_KAFKA_CONF("metadata.broker.list", _k_brokers);
    SET_KAFKA_CONF("group.id", _k_group_id);
    SET_KAFKA_CONF("client.id", _k_client_id);
    SET_KAFKA_CONF("enable.partition.eof", "false");
    SET_KAFKA_CONF("enable.auto.offset.store", "false");
    // TODO: set it larger than 0 after we set rd_kafka_conf_set_stats_cb()
    SET_KAFKA_CONF("statistics.interval.ms", "0");

    // create consumer
    _k_consumer = RdKafka::KafkaConsumer::create(conf, errstr); 
    if (!_k_consumer) {
        LOG(WARNING) << "failed to create kafka consumer";
        return Status("failed to create kafka consumer");
    }

    // create TopicPartitions
    std::vector<TopicPartition*> topic_partitions;
    std::stringstream ss;
    for (auto& entry : _k_partition_offset) {
        TopicPartition* tp1 = TopicPartition::create(_k_topic, entry.first);
        tp1->set_offset(entry.second);
        topic_partitions.push_back(tp1);
        ss << "partition[" << entry.first << ": " << entry.second << "];";   
    }

    RdKafka::ErrorCode err = _k_consumer->assign(topic_partitions);
    if (err) {
        LOG(WARNING) << "failed to assign topic partitions: " << ss.str()
                << ", err: " << RdKafka::err2str(err);
        return Status("failed to assgin topic partitions");
    }

    // delete TopicPartition finally
    std::for_each(topic_partitions.begin(), topic_partitions.end(),
            [](TopicPartition* tp1) { delete tp1};)

    VLOG(3) << "finished to init kafka consumer"
            << ". brokers=" << _k_brokers
            << ", group_id=" << _k_group_id
            << ", client_id=" << _k_client_id
            << ", topic=" << _k_topic
            << ", partition= " << ss.str();

    _init = true;
    return Status::OK;
}

Status KafkaConsumer::start() {

    int64_t left_time = _max_interval_ms;
    int64_t left_rows = _max_batch_rows;
    int64_t left_size = _max_batch_size;

    MonotonicStopWatch watch;
    watch.start();
    Status st;
    while (true) {
        std::unique_lock<std::mutex> l(_lock);
        if (_cancelled) {
            st = Status::CANCELLED;
            break;
        }

        if (_finished) {
            st = Status::OK;
            break;
        }

        if (left_time <= 0 || left_rows <= 0 || left_size <=0) {
            VLOG(3) << "kafka consume batch finished"
                    << ". left time=" << left_time
                    << ", left rows=" << left_rows
                    << ", left size=" << left_size; 
            _kafka_consumer_pipe->finish();
            _finished = true;
            return Status::OK;
        }

        // consume 1 message
        RdKafka::Message *msg = consumer->consume(1000 /* timeout, ms */);
        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                VLOG(3) << "get kafka message, offset: " << msg->offset();
                st = _kafka_consumer_pipe->append_with_line_delimiter(
                        static_cast<const char *>(msg->payload()),
                        static_cast<size_t>(msg->len()));
                if (st.ok()) {
                    left_rows--;
                    left_size -= msg->len();
                }

                break;
            case RdKafka::ERR__TIMED_OUT:
                // leave the status as OK, because this may happend
                // if there is no data in kafka.
                LOG(WARNING) << "kafka consume timeout";
                break;
            default:
                st = Status(msg->errstr());
                break;
        }
        delete msg; 

        if (!st.ok()) {
            _kafka_consumer_pipe->cancel();
            return st;
        }

        left_time = _max_interval_s - watch.elapsed_time() / 1000 / 1000 / 1000; 
    }

    return Status::OK;
}

Status KafkaConsumer::cancel() {
    std::unique_lock<std::mutex> l(_lock);
    if (!_init) {
        return Status("consumer not being initialized");
    }

    if (_finished) {
        return Status("consumer is already finished");
    }

    _cancelled = true;
    return Status::OK;
}

};

} // end namespace doris
