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

#ifndef BDG_PALO_BE_SRC_RUNTIME_KAFKA_COMSUMER_PIPE_H
#define BDG_PALO_BE_SRC_RUNTIME_KAFKA_COMSUMER_PIPE_H

#include <stdint.h>

#include <string>
#include <map>
#include <vector>

#include "librdkafka/rdkafka.h"

#include "exec/file_reader.h"
#include "http/message_body_sink.h"

namespace palo {

class KafkaConsumerPipe : public MessageBodySink, public FileReader {
public:
    KafkaConsumerPipe();
    ~KafkaConsumerPipe();


private:
    // this is only for testing librdkafka.a
    void test_kafka_lib() {
        //rd_kafka_conf_t *conf = rd_kafka_conf_new();
        //rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();
    }
};

} // end namespace palo

#endif // BDG_PALO_BE_SRC_RUNTIME_KAFKA_COMSUMER_PIPE_H
