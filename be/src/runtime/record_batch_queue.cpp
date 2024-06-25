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

#include "runtime/record_batch_queue.h"

#include "pipeline/dependency.h"
#include "util/spinlock.h"

namespace doris {

bool RecordBatchQueue::blocking_get(std::shared_ptr<arrow::RecordBatch>* result) {
    auto res = _queue.blocking_get(result);
    if (_dep && size() <= 10) {
        _dep->set_ready();
    }
    return res;
}

void RecordBatchQueue::update_status(const Status& status) {
    if (status.ok()) {
        return;
    }
    {
        std::lock_guard<SpinLock> l(_status_lock);
        if (_status.ok()) {
            _status = status;
        }
    }
}

void RecordBatchQueue::shutdown() {
    _queue.shutdown();
}

} // namespace doris
