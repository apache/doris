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

#include <glog/logging.h>
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/hdfs.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "util/runtime_profile.h"

namespace doris {
class THdfsParams;

namespace io {
struct FileInfo;

class HdfsFileSystemHandle {
public:
    HdfsFileSystemHandle(hdfsFS fs, bool cached, bool is_kerberos)
            : hdfs_fs(fs),
              from_cache(cached),
              _is_kerberos(is_kerberos),
              _ref_cnt(0),
              _create_time(_now()),
              _last_access_time(0),
              _invalid(false) {}

    ~HdfsFileSystemHandle() {
        DCHECK(_ref_cnt == 0);
        if (hdfs_fs != nullptr) {
            // DO NOT call hdfsDisconnect(), or we will meet "Filesystem closed"
            // even if we create a new one
            // hdfsDisconnect(hdfs_fs);
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

    bool invalid() {
        return _invalid ||
               (_is_kerberos &&
                _now() - _create_time.load() > config::kerberos_expiration_time_seconds * 1000 / 2);
    }

    void set_invalid() { _invalid = true; }

    hdfsFS hdfs_fs;
    // When cache is full, and all handlers are in use, HdfsFileSystemCache will return an uncached handler.
    // Client should delete the handler in such case.
    const bool from_cache;

private:
    const bool _is_kerberos;
    // the number of referenced client
    std::atomic<int> _ref_cnt;
    // For kerberos authentication, we need to save create time so that
    // we can know if the kerberos ticket is expired.
    std::atomic<uint64_t> _create_time;
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

class HdfsFileHandleCache;
class HdfsFileSystem final : public RemoteFileSystem {
public:
    static Status create(const THdfsParams& hdfs_params, const std::string& path,
                         RuntimeProfile* profile, std::shared_ptr<HdfsFileSystem>* fs);

    ~HdfsFileSystem() override;

    HdfsFileSystemHandle* get_handle();

    friend class HdfsFileHandleCache;

protected:
    Status connect_impl() override;
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;
    Status open_file_internal(const FileDescription& fd, const Path& abs_path,
                              FileReaderSPtr* reader) override;
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override;
    Status delete_file_impl(const Path& file) override;
    Status delete_directory_impl(const Path& dir) override;
    Status batch_delete_impl(const std::vector<Path>& files) override;
    Status exists_impl(const Path& path, bool* res) const override;
    Status file_size_impl(const Path& file, int64_t* file_size) const override;
    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;
    Status rename_impl(const Path& orig_name, const Path& new_name) override;
    Status rename_dir_impl(const Path& orig_name, const Path& new_name) override;

    Status upload_impl(const Path& local_file, const Path& remote_file) override;
    Status batch_upload_impl(const std::vector<Path>& local_files,
                             const std::vector<Path>& remote_files) override;
    Status direct_upload_impl(const Path& remote_file, const std::string& content) override;
    Status upload_with_checksum_impl(const Path& local_file, const Path& remote_file,
                                     const std::string& checksum) override;
    Status download_impl(const Path& remote_file, const Path& local_file) override;
    Status direct_download_impl(const Path& remote_file, std::string* content) override;

private:
    Status delete_internal(const Path& path, int is_recursive);

private:
    friend class HdfsFileWriter;
    HdfsFileSystem(const THdfsParams& hdfs_params, const std::string& path,
                   RuntimeProfile* profile);
    const THdfsParams& _hdfs_params;
    std::string _fs_name;
    // do not use std::shared_ptr or std::unique_ptr
    // _fs_handle is managed by HdfsFileSystemCache
    HdfsFileSystemHandle* _fs_handle;
    RuntimeProfile* _profile;
};
} // namespace io
} // namespace doris
