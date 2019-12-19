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

#ifndef DORIS_RECORD_BATCH_QUEUE_H
#define DORIS_RECORD_BATCH_QUEUE_H

#include "common/status.h"
#include "util/blocking_queue.hpp"

namespace arrow {

class RecordBatch;
}

namespace doris {

class RecordBatchQueue {
public:
    RecordBatchQueue() : _queue(config::max_memory_sink_batch_count) {}

    Status status() {
        std::lock_guard<std::mutex> l(_status_lock);
        return _status;
    }

    void update_status(const Status& status);

    bool blocking_get(std::shared_ptr<arrow::RecordBatch>* result) {
        return _queue.blocking_get(result);
    }

    bool blocking_put(const std::shared_ptr<arrow::RecordBatch>& val) {
        return _queue.blocking_put(val);
    }

    void clear();

private:
    BlockingQueue<std::shared_ptr<arrow::RecordBatch>> _queue;
    std::mutex _status_lock;
    Status _status;
};

}

#endif //DORIS_RECORD_BATCH_QUEUE_H
