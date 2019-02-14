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

#include <string>
#include <map>
#include <vector>

namespace doris {

class KafkaConsumerPipe;
class RdKafka::KafkaConsumer;

class KafkaConsumer {
public:
    KafkaConsumer(
            const std::string& brokers,
            const std::string& group_id,
            const std::string& client_id,
            const std::string& topic,
            const std::map<int64_t, int64_t>& partition_offset,
            std::shared_ptr<KafkaConsumerPipe> kafka_consumer_pipe, 
            ):
        _k_brokers(broker),
        _k_group_id(group_id),
        _k_client_id(client_id),
        _k_topic(topic),
        _k_partition_offset(partition_offset),
        _kafka_consumer_pipe(kafka_consumer_pipe),
        _init(false),
        _finished(false),
        _cancelled(false) {
        
    }

    // init the KafkaConsumer with the given parameters
    Status init();

    // start consuming
    Status start();

    // cancel the consuming process.
    // if the consumer is not initialized, or the consuming
    // process is already finished, call cancel() will
    // return ERROR
    Status cancel();

    ~KafkaConsumer() {
        if (_consumer) {
            _consumer->close();
            delete _consumer;
        }
    }

private:

    std::string _k_brokers;
    std::string _k_group_id;
    std::string _k_client_id;
    std::string _k_topic;
    // partition id -> offset
    std::map<int64_t, int64_t> _k_partition_offset;
    std::shared_ptr<KafkaConsumerPipe> _kafka_consumer_pipe;
    RdKafka::KafkaConsumer* _consumer;

    // lock to protect the following bools
    std::mutex _lock
    bool _init;
    bool _finished;
    bool _cancelled;
};

} // end namespace doris
