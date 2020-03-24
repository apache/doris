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

#include "util/file_cache.h"
#include "env/env.h"
#include "common/config.h"

namespace doris {

class Env;

// FileManager to control file operation
// Now, only support RandomAccessFile for read operation for easy.
class FileManager {
public:
    FileManager(Env* env) : _env(env),
            _file_cache(new FileCache<RandomAccessFile>("Readable file cache", config::file_descriptor_cache_capacity)) { }

    static FileManager* instance();

    // Destroys the file cache.
    ~FileManager() { }

    Status open_file(const std::string& file_name, OpenedFileHandle<RandomAccessFile>* file_handle);

private:
    Env* _env;

    // Underlying cache instance. Caches opened files.
    std::unique_ptr<FileCache<RandomAccessFile>> _file_cache;

    DISALLOW_COPY_AND_ASSIGN(FileManager);
};

} // namespace doris
