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

#include "io/fs/file_system_map.h"

#include <mutex>

namespace doris {
namespace io {

FileSystemMap* FileSystemMap::instance() {
    static FileSystemMap map;
    return &map;
}

void FileSystemMap::insert(ResourceId id, FileSystemSPtr fs) {
    std::unique_lock wlock(_mu);
    _map.try_emplace(std::move(id), std::move(fs));
}

FileSystemSPtr FileSystemMap::get(const ResourceId& id) {
    std::shared_lock rlock(_mu);
    auto it = _map.find(id);
    if (it != _map.end()) {
        return it->second;
    }
    return nullptr;
}

} // namespace io
} // namespace doris
