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

#include "util/file_manager.h"

namespace doris {

FileManager* FileManager::instance() {
    static FileManager file_manager(Env::Default());
    return &file_manager;
}

Status FileManager::open_file(const std::string& file_name, OpenedFileHandle<RandomAccessFile>* file_handle) {
    bool found = _file_cache->lookup(file_name, file_handle);
    if (found) {
        return Status::OK();
    }
    std::unique_ptr<RandomAccessFile> file;
    RETURN_IF_ERROR(_env->new_random_access_file(file_name, &file));
    _file_cache->insert(file_name, file.release(), file_handle);
    return Status::OK();
}

} // namespace doris
