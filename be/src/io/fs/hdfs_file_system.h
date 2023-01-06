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

#include <gen_cpp/PlanNodes_types.h>
#include <hdfs/hdfs.h>

#include <atomic>

#include "io/fs/remote_file_system.h"
namespace doris {

namespace io {

class HdfsFileSystemHandle {
public:
    HdfsFileSystemHandle(hdfsFS fs, bool cached)
            : hdfs_fs(fs), from_cache(cached), _ref_cnt(0), _last_access_time(0), _invalid(false) {}

    ~HdfsFileSystemHandle() {
        DCHECK(_ref_cnt == 0);
        if (hdfs_fs != nullptr) {
            // Even if there is an error, the resources associated with the hdfsFS will be freed.
            hdfsDisconnect(hdfs_fs);
        }
        hdfs_fs = nullptr;
    }

    int64_t last_access_time() { return _last_access_time; }

    void inc_ref() {
        _ref_cnt++;
        _last_access_time = _now();
    }

    void dec_ref() {
        _ref_cnt--;
        _last_access_time = _now();
    }

    int ref_cnt() { return _ref_cnt; }

    bool invalid() { return _invalid; }

    void set_invalid() { _invalid = true; }

    hdfsFS hdfs_fs;
    // When cache is full, and all handlers are in use, HdfsFileSystemCache will return an uncached handler.
    // Client should delete the handler in such case.
    const bool from_cache;

private:
    // the number of referenced client
    std::atomic<int> _ref_cnt;
    // HdfsFileSystemCache try to remove the oldest handler when the cache is full
    std::atomic<uint64_t> _last_access_time;
    // Client will set invalid if error thrown, and HdfsFileSystemCache will not reuse this handler
    std::atomic<bool> _invalid;

    uint64_t _now() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
    }
};

class HdfsFileSystem final : public RemoteFileSystem {
public:
    HdfsFileSystem(const THdfsParams& hdfs_params, const std::string& path);
    ~HdfsFileSystem() override;

    Status create_file(const Path& path, FileWriterPtr* writer) override;

    Status open_file(const Path& path, FileReaderSPtr* reader) override;

    Status delete_file(const Path& path) override;

    Status create_directory(const Path& path) override;

    // Delete all files under path.
    Status delete_directory(const Path& path) override;

    Status link_file(const Path& /*src*/, const Path& /*dest*/) override {
        return Status::NotSupported("Not supported");
    }

    Status exists(const Path& path, bool* res) const override;

    Status file_size(const Path& path, size_t* file_size) const override;

    Status list(const Path& path, std::vector<Path>* files) override;

    Status upload(const Path& /*local_path*/, const Path& /*dest_path*/) override {
        return Status::NotSupported("Currently not support to upload file to HDFS");
    }

    Status batch_upload(const std::vector<Path>& /*local_paths*/,
                        const std::vector<Path>& /*dest_paths*/) override {
        return Status::NotSupported("Currently not support to batch upload file to HDFS");
    }

    Status connect() override;

    HdfsFileSystemHandle* get_handle();

private:
    Path _covert_path(const Path& path) const;
    const THdfsParams& _hdfs_params;
    std::string _namenode;
    // do not use std::shared_ptr or std::unique_ptr
    // _fs_handle is managed by HdfsFileSystemCache
    HdfsFileSystemHandle* _fs_handle;
};
} // namespace io
} // namespace doris
