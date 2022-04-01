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

struct FileStat {
    std::string name;
    std::string md5;
    int64_t size;
};

class FileSystem {
public:
    // Check if the specified file exists.
    virtual Status exists(const String& path, bool* res) const = 0;

    // Check if the specified file exists and it's a regular file (not a directory or special file type).
    virtual Status is_file(const String& path, bool* res) const = 0;

    // Check if the specified file exists and it's a directory.
    virtual Status is_directory(const String& path, bool* res) const = 0;

    // Get all files under the *path* folder
    virtual Status list(const std::string& path, bool contain_md5, bool recursion, std::map<std::string, FileStat>* files) = 0;
    
    // Delete the directory if it is exist and not a regular file
    // If it is a file, return error
    // If the directory not exist, return success
    virtual Status delete_directory(const std::string& path) = 0;

    // Delete file if it is exist and not a directory
    // If the file is a directory return error
    // If the file not exist just return success
    virtual Status delete_file(const std::string& path) = 0;

    // Open the file for read and return ReadBufferFromFileBase object.
    virtual std::unique_ptr<ReadStream> read_file(const std::string & path, IOContext io_context) const = 0;

    // Open the file for write and return WriteBufferFromFileBase object.
    // Use unique ptr because the stream not support concurrency
    virtual std::unique_ptr<WriteStream> write_file(const std:string & path, IOContext io_context) = 0;

    virtual ~FileSystem() = default;
};

} // end namespace doris
