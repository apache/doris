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

#include <librdkafka/rdkafka.h>

#include "core/custom_allocator.h"
#include "load/routine_load/data_consumer.h"

namespace doris {

class KafkaShareConsumer : public DataConsumer {
public:
    ~KafkaShareConsumer() override;

    Status init(std::shared_ptr<StreamLoadContext> ctx) override;
    Status consume(std::shared_ptr<StreamLoadContext> ctx) override;
    Status cancel(std::shared_ptr<StreamLoadContext> ctx) override;
    Status reset() override;
    bool match(std::shared_ptr<StreamLoadContext> ctx) override;
    Status acknowledge_committed();

protected:
    virtual Status acknowledge_record(int32_t partition, int64_t offset, bool accept);
    virtual Status commit_acknowledgements();

private:
    Status acknowledge(bool committed);

    rd_kafka_share_t* _consumer = nullptr;
    DorisVector<std::pair<int32_t, int64_t>> _records;
    size_t _processed = 0;
    bool _reusable = true;
    std::string _brokers;
    std::string _topic;
    std::map<std::string, std::string> _properties;
};

} // namespace doris
