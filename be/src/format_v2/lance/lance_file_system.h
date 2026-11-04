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

#include <lance/lance.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"

namespace doris {
class RuntimeState;
namespace format::lance {

// Adapts Lance's byte-range requests to Doris storage; it owns no execution threads.
class LanceFileSystem final {
public:
    struct Statistics {
        uint64_t bytes_from_cache = 0;
        uint64_t bytes_from_remote = 0;
    };

    static Result<std::shared_ptr<LanceFileSystem>> create(const std::string& uri,
                                                           const char* const* storage_options,
                                                           RuntimeState* state = nullptr);
    LanceHostStorageOptionsV1 options();
    Statistics statistics() const;

private:
    struct OpenFile;
    LanceFileSystem() = default;
    Status initialize(const std::string& uri, const char* const* storage_options,
                      RuntimeState* state);
    Status filesystem(const std::string& uri, io::FileSystemSPtr* fs, std::string* path);
    Status open_file(const char* uri, OpenFile** file, LanceHostFileMeta* metadata);
    Status list_files(const char* uri, LanceHostListEntry emit, void* context);
    Status check_cancelled() const;
    static int32_t report(const Status& status, const char** error) noexcept;
    static void retain(void* context);
    static void release(void* context);
    static int32_t open(void*, const char*, void**, LanceHostFileMeta*, const char**) noexcept;
    static int32_t read_at(void*, uint64_t, uint8_t*, size_t, size_t*, const char**) noexcept;
    static void close(void*) noexcept;
    static int32_t list(void*, const char*, LanceHostListEntry, void*, const char**) noexcept;

    std::atomic<size_t> _refs {1};
    std::map<std::string, std::string> _properties;
    std::string _storage_namespace;
    std::string _cache_namespace;
    TQueryOptions _query_options;
    std::mutex _fs_mutex;
    std::map<std::string, io::FileSystemSPtr> _filesystems;
    std::map<std::string, LanceHostFileMeta> _immutable_metadata;
    std::atomic<uint64_t> _cache_bytes {0};
    std::atomic<uint64_t> _remote_bytes {0};
};

} // namespace format::lance
} // namespace doris
