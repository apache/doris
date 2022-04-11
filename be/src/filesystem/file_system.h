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

#include "common/status.h"
#include "filesystem/io_context.h"

namespace doris {

class ReadStream;
class WriteStream;

struct FileStat {
    std::string name;
    size_t size;
};

class FileSystem {
public:
    virtual ~FileSystem() = default;

    // Check if the specified file exists.
    virtual Status exists(const std::string& path, bool* res) const = 0;

    // Check if the specified file exists and it's a regular file (not a directory or special file type).
    virtual Status is_file(const std::string& path, bool* res) const = 0;

    // Check if the specified file exists and it's a directory.
    virtual Status is_directory(const std::string& path, bool* res) const = 0;

    // Get all files under the `path` directory.
    // If it's not a directory, return error
    virtual Status list(const std::string& path, std::vector<FileStat>* files) = 0;

    // Delete the directory recursively if it exists and not a regular file.
    // If it's a file, return error
    // If the directory doesn't exist, return ok
    virtual Status delete_directory(const std::string& path) = 0;

    // Delete file if it exists and not a directory.
    // If it's a directory, return error
    // If the file doesn't exist, return ok
    virtual Status delete_file(const std::string& path) = 0;

    // Create directory recursively.
    // If `path` exists, return error
    virtual Status create_directory(const std::string& path) = 0;

    // Open the file for read and return ReadStream object.
    virtual Status read_file(const std::string& path, IOContext io_context,
                             std::unique_ptr<ReadStream>* stream) const = 0;

    // Open the file for write and return WriteStream object.
    virtual Status write_file(const std::string& path, IOContext io_context,
                              std::unique_ptr<WriteStream>* stream) = 0;
};

} // namespace doris
