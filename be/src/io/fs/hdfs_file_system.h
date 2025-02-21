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
#include "io/fs/hdfs.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "util/runtime_profile.h"

namespace doris {
class THdfsParams;

namespace io {
struct FileInfo;

class HdfsHandler;
class HdfsFileHandleCache;
class HdfsFileSystem final : public RemoteFileSystem {
public:
    static Status create(const THdfsParams& hdfs_params, std::string id, const std::string& path,
                         RuntimeProfile* profile, std::shared_ptr<HdfsFileSystem>* fs);

    ~HdfsFileSystem() override;

    friend class HdfsFileHandleCache;

protected:
    Status connect_impl() override;
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* opts) override;
    Status open_file_internal(const Path& file, FileReaderSPtr* reader,
                              const FileReaderOptions& opts) override;
    Status create_directory_impl(const Path& dir, bool failed_if_exists = false) override;
    Status delete_file_impl(const Path& file) override;
    Status delete_directory_impl(const Path& dir) override;
    Status batch_delete_impl(const std::vector<Path>& files) override;
    Status exists_impl(const Path& path, bool* res) const override;
    Status file_size_impl(const Path& file, int64_t* file_size) const override;
    Status list_impl(const Path& dir, bool only_file, std::vector<FileInfo>* files,
                     bool* exists) override;
    Status rename_impl(const Path& orig_name, const Path& new_name) override;

    Status upload_impl(const Path& local_file, const Path& remote_file) override;
    Status batch_upload_impl(const std::vector<Path>& local_files,
                             const std::vector<Path>& remote_files) override;
    Status download_impl(const Path& remote_file, const Path& local_file) override;

private:
    Status delete_internal(const Path& path, int is_recursive);

private:
    friend class HdfsFileWriter;
    HdfsFileSystem(const THdfsParams& hdfs_params, std::string id, const std::string& path,
                   RuntimeProfile* profile);
    const THdfsParams& _hdfs_params;
    std::string _fs_name;
    std::shared_ptr<HdfsHandler> _fs_handler = nullptr;
    RuntimeProfile* _profile = nullptr;
};
} // namespace io
} // namespace doris
