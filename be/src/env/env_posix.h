//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

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

    Status new_sequential_file(const std::string& fname,
                               std::unique_ptr<SequentialFile>* result) override;

    // get a RandomAccessFile pointer without file cache
    Status new_random_access_file(const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override;

    Status new_random_access_file(const RandomAccessFileOptions& opts, const std::string& fname,
                                  std::unique_ptr<RandomAccessFile>* result) override;

    Status new_writable_file(const std::string& fname, std::unique_ptr<WritableFile>* result) override;

    Status new_writable_file(const WritableFileOptions& opts, const std::string& fname,
                             std::unique_ptr<WritableFile>* result) override;

    Status new_random_rw_file(const std::string& fname, std::unique_ptr<RandomRWFile>* result) override;

    Status new_random_rw_file(const RandomRWFileOptions& opts, const std::string& fname,
                              std::unique_ptr<RandomRWFile>* result) override;

    Status path_exists(const std::string& fname, bool is_dir = false) override;

    Status get_children(const std::string& dir, std::vector<std::string>* result) override;

    Status iterate_dir(const std::string& dir,
                       const std::function<bool(const char*)>& cb) override;

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

    bool is_remote_env() override {
        return false;
    }
};

} // namespace doris