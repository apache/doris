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

#include "env/env.h"

namespace doris {

class RandomAccessFile;
class RandomRWFile;
class WritableFile;
class SequentialFile;
struct WritableFileOptions;
struct RandomAccessFileOptions;
struct RandomRWFileOptions;

class PosixEnv : public Env {
public:
    ~PosixEnv() override {}

    Status init_conf() override;

    Status new_sequential_file(const std::string& fname,
                               std::unique_ptr<SequentialFile>* result) override;

    // get a RandomAccessFile pointer without file cache
    Status new_random_access_file(const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override;

    Status new_random_access_file(const RandomAccessFileOptions& opts, const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override;

    Status new_writable_file(const std::string& fname,
                             std::unique_ptr<WritableFile>* result) override;

    Status new_writable_file(const WritableFileOptions& opts, const std::string& fname,
                             std::unique_ptr<WritableFile>* result) override;

    Status new_random_rw_file(const std::string& fname,
                              std::unique_ptr<RandomRWFile>* result) override;

    Status new_random_rw_file(const RandomRWFileOptions& opts, const std::string& fname,
                              std::unique_ptr<RandomRWFile>* result) override;

    Status path_exists(const std::string& fname, bool is_dir = false) override;

    Status get_children(const std::string& dir, std::vector<std::string>* result) override;

    Status iterate_dir(const std::string& dir, const std::function<bool(const char*)>& cb) override;

    Status delete_file(const std::string& fname) override;

    Status create_dir(const std::string& name) override;

    Status create_dir_if_missing(const std::string& dirname, bool* created = nullptr) override;

    Status create_dirs(const std::string& dirname) override;

    // Delete the specified directory.
    Status delete_dir(const std::string& dirname) override;

    Status sync_dir(const std::string& dirname) override;

    Status is_directory(const std::string& path, bool* is_dir) override;

    Status canonicalize(const std::string& path, std::string* result) override;

    Status get_file_size(const std::string& fname, uint64_t* size) override;

    Status get_file_modified_time(const std::string& fname, uint64_t* file_mtime) override;

    Status copy_path(const std::string& src, const std::string& target) override;

    Status rename_file(const std::string& src, const std::string& target) override;

    Status rename_dir(const std::string& src, const std::string& target) override;

    Status link_file(const std::string& old_path, const std::string& new_path) override;

    Status get_space_info(const std::string& path, int64_t* capacity, int64_t* available) override;

    bool is_remote_env() override { return false; }
};

} // namespace doris