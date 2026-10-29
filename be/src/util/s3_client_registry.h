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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "common/status.h"

namespace doris {

class ObjStorageClient;
struct S3ClientConf;

// Reuse clients only while real users own them. Neither the weak index nor a
// completed creation keeps an idle SDK client/connection pool alive.
class S3ClientRegistry {
public:
    using Client = std::shared_ptr<ObjStorageClient>;
    using Creator = std::function<Result<Client>(const S3ClientConf&)>;

    struct Stats {
        size_t indexed;
        size_t inflight;
        bool closed;
        int64_t hits;
        int64_t misses;
        int64_t waits;
        int64_t created;
        int64_t destroyed;
        int64_t live;
        int64_t failures;
    };

    S3ClientRegistry();
    ~S3ClientRegistry();

    S3ClientRegistry(const S3ClientRegistry&) = delete;
    S3ClientRegistry& operator=(const S3ClientRegistry&) = delete;

    // creator returns a new client. Preserve its original control block so
    // shared_from_this(), iterators and asynchronous users share its lifetime.
    Result<Client> get_or_create(const S3ClientConf& conf, const Creator& creator);
    Stats stats() const;

    // Stop acquisitions and wait for creators after request producers stop.
    // Existing users continue to own valid clients until they release them.
    void close();
    void expose_metrics();

private:
    struct Impl;
    std::shared_ptr<Impl> _impl;
};

} // namespace doris
