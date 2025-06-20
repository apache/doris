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
#include <string>

#include "common/status.h"
#include "vec/exec/executor/listenable_future.h"

namespace doris {
namespace vectorized {

class SplitRunner {
public:
    virtual ~SplitRunner() = default;
    virtual Status init() = 0;
    virtual Result<SharedListenableFuture<Void>> process_for(std::chrono::nanoseconds duration) = 0;
    virtual void close(const Status& status) = 0;
    virtual bool is_finished() = 0;
    virtual Status finished_status() = 0;
    virtual std::string get_info() const = 0;
    virtual bool is_auto_reschedule() const { return true; }
};

} // namespace vectorized
} // namespace doris
