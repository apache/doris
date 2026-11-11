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

#include <cstdint>
#include <string>

namespace doris {
class RuntimeProfile;
class RuntimeState;
namespace io {
struct FileSystemProperties;
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::format::paimon {

// Borrowed for one materialize() call. Scanner-owned profile/statistics must outlive
// split-owned readers, including their final profile collection.
struct PaimonBlobReadContext {
    const io::FileSystemProperties& system_properties;
    RuntimeState* runtime_state;
    const io::IOContext* io_ctx;
    RuntimeProfile* profile;
    const std::string& fs_name;
    bool file_cache_admission;
    int64_t blob_max_size;
};

} // namespace doris::format::paimon
