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
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "util/threadpool.h"

namespace Aws::Utils::Threading {
class Executor;
} // namespace Aws::Utils::Threading

namespace doris::io {
// Serve as the io executor for all the S3 operation including aws S3, google cloud storage
// COS, OSS, OBS
class AwsIOExecutor final : public Aws::Utils::Threading::Executor {
public:
    AwsIOExecutor(std::shared_ptr<ThreadPool> thread_pool, std::string name)
            : _thread_pool(std::move(thread_pool)), _name(std::move(name)) {}
    ~AwsIOExecutor() override = default;
    bool SubmitToThread(std::function<void()>&& task) override {
        if (Status s = _thread_pool->submit_func(std::move(task)); !s.ok()) {
            LOG_WARNING("Failed to summit to remote io executor {} because {}", _name, s);
            return false;
        }
        return true;
    }

private:
    std::shared_ptr<ThreadPool> _thread_pool;
    std::string _name;
};
} // namespace doris::io