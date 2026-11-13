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

#include "common/config.h"
#include "common/status.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "runtime/exec_env.h"

namespace doris::test {

// Provide a populated cache even when the test runs alone or follows a fixture that disabled caching.
// Callers must release cache users and finish asynchronous writes before destroying this scope.
class ScopedFileCache {
public:
    ScopedFileCache()
            : _previous_factory(ExecEnv::GetInstance()->file_cache_factory()),
              _previous_enable_file_cache(config::enable_file_cache) {}

    ~ScopedFileCache() {
        ExecEnv::GetInstance()->set_file_cache_factory(_previous_factory);
        config::enable_file_cache = _previous_enable_file_cache;
    }

    ScopedFileCache(const ScopedFileCache&) = delete;
    ScopedFileCache& operator=(const ScopedFileCache&) = delete;

    Status init() {
        // Standalone runs have no factory, and earlier fixtures can leave an empty one.
        // Reuse a populated factory: memory caches share the same path and metric names.
        if (_previous_factory == nullptr || _previous_factory->get_cache_instance_size() == 0) {
            auto settings = io::get_file_cache_settings(16 * 1024 * 1024, 0);
            settings.storage = "memory";
            RETURN_IF_ERROR(_factory.create_file_cache("memory", settings));
            ExecEnv::GetInstance()->set_file_cache_factory(&_factory);
        }
        config::enable_file_cache = true;
        return Status::OK();
    }

private:
    io::FileCacheFactory* _previous_factory;
    bool _previous_enable_file_cache;
    io::FileCacheFactory _factory;
};

} // namespace doris::test
