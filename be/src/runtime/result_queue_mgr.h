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
#include "runtime/primitive_type.h"
#include "runtime/raw_value.h"
#include "runtime/record_batch_queue.h"
#include "util/hash_util.hpp"

namespace arrow {

class RecordBatch;
}

namespace doris {

class TUniqueId;
class TScanRowBatch;
class RecordBatchQueue;
typedef std::shared_ptr<RecordBatchQueue> BlockQueueSharedPtr;

class ResultQueueMgr {
public:
    ResultQueueMgr();
    ~ResultQueueMgr();

    Status fetch_result(const TUniqueId& fragment_instance_id,
                        std::shared_ptr<arrow::RecordBatch>* result, bool* eos);

    void create_queue(const TUniqueId& fragment_instance_id, BlockQueueSharedPtr* queue);

    Status cancel(const TUniqueId& fragment_id);

    void update_queue_status(const TUniqueId& fragment_id, const Status& status);

private:
    std::mutex _lock;
    std::unordered_map<TUniqueId, BlockQueueSharedPtr> _fragment_queue_map;
};

} // namespace doris
