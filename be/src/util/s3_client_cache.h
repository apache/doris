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

#include <functional>
#include <memory>

#include "util/s3_util.h"

namespace doris {

// Retains only a bounded, recently used set of clients. Callers own independent shared_ptrs,
// so removing a cache entry never invalidates a reader or writer that is still using it.
class S3ClientCache {
public:
    using Client = std::shared_ptr<io::ObjStorageClient>;
    using Creator = std::function<Result<Client>()>;
    using Clock = std::function<int64_t()>;

    S3ClientCache(size_t capacity, int64_t idle_ttl_seconds, Clock clock);
    ~S3ClientCache();

    Result<Client> get_or_create(const S3ClientConf& conf, const Creator& creator);
    size_t prune_stale();
    size_t clear();
    size_t size() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris
