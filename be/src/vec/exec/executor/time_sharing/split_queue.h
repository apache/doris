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

#include <memory>
#include <vector>

#include "vec/exec/executor/time_sharing/prioritized_split_runner.h"
#include "vec/exec/executor/time_sharing/priority.h"

namespace doris {
namespace vectorized {

class SplitQueue {
public:
    virtual ~SplitQueue() = default;

    virtual int compute_level(int64_t scheduled_nanos) = 0;
    virtual void offer(std::shared_ptr<PrioritizedSplitRunner> split) = 0;
    virtual std::shared_ptr<PrioritizedSplitRunner> take() = 0;
    virtual size_t size() const = 0;
    virtual void remove(std::shared_ptr<PrioritizedSplitRunner> split) = 0;
    virtual void remove_all(const std::vector<std::shared_ptr<PrioritizedSplitRunner>>& splits) = 0;
    virtual void clear() = 0;

    virtual Priority update_priority(const Priority& old_priority, int64_t quanta_nanos,
                                     int64_t scheduled_nanos) = 0;

    virtual int64_t get_level_min_priority(int level, int64_t scheduled_nanos) = 0;
};

} // namespace vectorized
} // namespace doris
