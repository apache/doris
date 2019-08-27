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

#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "util/blocking_queue.hpp"
#include "util/hash_util.hpp"
#include "runtime/primitive_type.h"
#include "runtime/raw_value.h"

namespace doris {

class TUniqueId;
class TScanRowBatch;
typedef std::shared_ptr<BlockingQueue<std::shared_ptr<TScanRowBatch>>> shared_block_queue_t;

class ResultQueueMgr {

public:
    ResultQueueMgr();
    ~ResultQueueMgr();

    Status fetch_result(const TUniqueId& fragment_instance_id, std::shared_ptr<TScanRowBatch>* result, bool *eos);

    void create_queue(const TUniqueId& fragment_instance_id, shared_block_queue_t* queue);

    Status cancel(const TUniqueId& fragment_id);

private:
    std::mutex _lock;
    u_int32_t _max_sink_batch_count;
    std::unordered_map<TUniqueId, shared_block_queue_t> _fragment_queue_map;
};

}
